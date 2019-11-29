import enum
from copy import copy
from functools import partial
from itertools import chain, groupby
import logging
from queue import Queue
from typing import Callable, Any, List, Tuple, Iterable

from s3rsync.session import Session
from s3rsync.history import RemoteNodeHistory
from s3rsync.models import StoredNodeHistory, RootFolder
from s3rsync.node import LocalNode
from s3rsync.sync_action import SyncActionExecutor, SyncAction
from s3rsync.sync_logic import handle_node
from s3rsync.s3util import list_versions
from s3rsync.util.timeout import Timeout
from s3rsync.util.row import Row
from s3rsync.util.file import iter_folder


class SyncWorkerEvent(str, enum.Enum):
    SCHEDULED_SYNC = "scheduled_sync"
    SYNC_ACTION = "sync_action"


class SyncWorker:
    def __init__(self, session: Session) -> None:
        self.session = session

        self.sync_timeout = Timeout(
            partial(self.schedule_event, SyncWorkerEvent.SCHEDULED_SYNC), interval=10
        )

        self.sync_action_producer = SyncActionProducer(session)
        self.sync_action_executor = SyncActionExecutor(session)

        self.event_queue: Any = Queue()
        self.sync_actions: List[Callable] = []

    def schedule_event(self, event: SyncWorkerEvent) -> None:
        self.event_queue.put(event)

    def run(self):
        self.schedule_event(SyncWorkerEvent.SCHEDULED_SYNC)

        while True:
            event = self.event_queue.get()
            if event == SyncWorkerEvent.SCHEDULED_SYNC:
                logging.info("[SYNC] Executing scheduled sync")
                self.do_sync()
            elif event == SyncWorkerEvent.SYNC_ACTION:
                self.do_sync_action()

    def run_once(self):
        logging.info("[SYNC] Running sync")
        self.sync_actions = self.sync_action_producer.produce()
        logging.info("[SYNC] Sync produced actions: %r", self.sync_actions)
        for action in self.sync_actions:
            logging.info("[SYNC] Executing sync action: %r", action)
            self.sync_action_executor.do_action(action)

    def do_sync(self):
        self.sync_timeout.stop()
        logging.info("[SYNC] Running sync")
        self.sync_actions = self.sync_action_producer.produce()
        logging.info("[SYNC] Sync produced actions: %r", self.sync_actions)
        self.schedule_event(SyncWorkerEvent.SYNC_ACTION)

    def do_sync_action(self):
        if self.sync_actions:
            action = self.sync_actions.pop(0)
            logging.info("[SYNC] Executing sync action: %r", action)
            self.sync_action_executor.do_action(action)
            self.schedule_event(SyncWorkerEvent.SYNC_ACTION)
        else:
            logging.info("[SYNC] Starting timer")
            self.sync_timeout.start()


class NodeRow(Row):
    value_types = [RemoteNodeHistory, LocalNode, StoredNodeHistory]


class HistoryRow(Row):
    value_types = [RemoteNodeHistory, StoredNodeHistory]


class SyncActionProducer:
    def __init__(self, session: Session):
        self.session = session

    def produce(self) -> List[SyncAction]:
        remote_history, stored_history = fetch_history(self.session)
        local_nodes = scan_local_files(self.session)

        all_nodes = list(chain(remote_history, local_nodes, stored_history))
        all_nodes.sort(key=lambda n: n.key)  # type: ignore
        rows = [
            NodeRow.create(key, nodes)
            for key, nodes in groupby(all_nodes, key=lambda n: n.key)  # type: ignore
        ]
        actions = []
        for _, remote, local, stored in rows:
            actions.append(handle_node(remote, local, stored))

        return actions


def fetch_history(session: Session) -> Tuple[List[RemoteNodeHistory], List[StoredNodeHistory]]:
    remote_history_versions = list_versions(
        session.s3_client,
        session.internal_bucket,
        f"{session.s3_prefix}/{session.sync_metadata_prefix}/history/",
    )
    remote_history = (
        RemoteNodeHistory.from_s3_object(v) for v in remote_history_versions
    )
    stored_history = StoredNodeHistory.select().where(
        StoredNodeHistory.root_folder == RootFolder.for_session(session)
    )
    all_history = list(chain(remote_history, stored_history))
    all_history.sort(key=lambda h: h.key)
    rows = [
        HistoryRow.create(key, history)
        for key, history in groupby(all_history, key=lambda h: h.key)
    ]
    for _, remote, stored in rows:
        if remote:
            if not stored or remote.etag != stored.remote_history_etag:
                remote.load(session)
            else:
                remote.history = copy(stored.history)

    return (
        [r for _, r, s in rows if r is not None],
        [s for _, _, s in rows if s is not None]
    )


def scan_local_files(session: Session) -> Iterable[LocalNode]:
    return (
        LocalNode.create(p, session) for p in iter_folder(session.root_folder.path)
    )
