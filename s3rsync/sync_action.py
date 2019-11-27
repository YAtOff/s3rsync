from dataclasses import dataclass
from functools import wraps, partial
from typing import Callable, Any

from s3rsync.session import Session
from s3rsync.history import RemoteNodeHistory
from s3rsync.node import LocalNode
from s3rsync.models import StoredNodeHistory, RootFolder
from s3rsync import s3util


@dataclass
class SyncActionResult:
    pass


class SyncAction:
    def __init__(self, action: Callable):
        self.action = action

    def __call__(self, *args, **kwargs):
        return self.action(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{self.action.func.__name__}({self.action.args, self.action.keywords})"  # type: ignore


class SyncActionExecutor:
    def __init__(self, session: Session):
        self.session = session

    def do_action(self, action: SyncAction) -> SyncActionResult:
        return action(self.session)


def action(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable[[Session], SyncActionResult]:
        return SyncAction(partial(func, *args, **kwargs))

    return wrapper


@action
def upload(
    history: RemoteNodeHistory, node: LocalNode, session: Session
) -> SyncActionResult:
    """
    1. Without remote history:
      - Calc signature
      - Generate id
      - Create new history
      - Upload base
      - Upload history
      - Store history in local DB

    2. With remote history:
      - Fetch signature
      - Calc delta
      - Calc signature
      - Generate id
      - Upload delta
      - Upload signature
      - Add history record
      - Upload history
      - Store history in local DB

    """
    return SyncActionResult()


@action
def download(
    remote_history: RemoteNodeHistory,
    stored_history: StoredNodeHistory,
    session: Session,
) -> SyncActionResult:
    """
    1. Without local history
      - Find latest base
      - Download latest base
      - Store history in local DB
    2. With local history
      - Diff remote and local history and find shortest path
      - Fetch base
      - Fetch deltas one by one pathc
      - Store history in local DB
    """
    return SyncActionResult()


@action
def delete_local(
    node: LocalNode, history: StoredNodeHistory, session: Session
) -> SyncActionResult:
    (node.root_folder / node.path).unlink()
    history.delete()
    return SyncActionResult()


@action
def delete_remote(
    remote_history: RemoteNodeHistory,
    stored_history: StoredNodeHistory,
    session: Session,
) -> SyncActionResult:
    history = remote_history.history
    s3util.delete_file(
        session.s3_client, session.sotrage_bucket,
        f"{session.s3_prefix}/{history.base_path}"
    )
    history.add_delete_marker()
    remote_history.save(session)
    stored_history.delete()
    return SyncActionResult()


@action
def save_history(
    remote_history: RemoteNodeHistory,
    node: LocalNode,
    session: Session
) -> SyncActionResult:
    StoredNodeHistory.create(
        key=remote_history.key,
        root_folder=RootFolder.for_session(session),
        data=remote_history.history.dict(),
        local_modified_time=node.created_time,
        local_created_time=node.modified_time,
        remote_history_etag=remote_history.etag
    )
    return SyncActionResult()


@action
def delete_history(history: StoredNodeHistory, session: Session) -> SyncActionResult:
    history.delete()
    return SyncActionResult()


@action
def conflict(
    remote_history: RemoteNodeHistory,
    node: LocalNode,
    stored_history: StoredNodeHistory,
    session: Session
) -> SyncActionResult:
    return SyncActionResult()


@action
def nop(opaque: Any, session: Session) -> SyncActionResult:
    return SyncActionResult()
