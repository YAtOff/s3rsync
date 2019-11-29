import os
from dataclasses import dataclass
from functools import partial, wraps
from pathlib import Path
from typing import Any, Callable, Optional, cast

from s3rsync import file_transfer, s3util
from s3rsync.history import NodeHistory, RemoteNodeHistory, NodeHistoryEntry
from s3rsync.models import RootFolder, StoredNodeHistory
from s3rsync.node import LocalNode
from s3rsync.rsync import calc_delta, calc_signature, patch_file
from s3rsync.session import Session
from s3rsync.util.file import create_temp_file


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
    remote_history: Optional[RemoteNodeHistory], node: LocalNode, session: Session
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
      - Generate key
      - Calc delta
      - Calc signature
      - Upload delta
      - Upload signature
      - Add history record
      - Upload history
      - Store history in local DB

    """
    new_key = NodeHistoryEntry.generate_key()
    if remote_history is not None:
        history = cast(NodeHistory, remote_history.history)
        with create_temp_file() as delta_path:
            calc_delta(session, node.local_fspath, history.last.key, delta_path)
            file_transfer.upload_metadata(session, delta_path, new_key, "delta")
            delta_size = Path(delta_path).stat().st_size
        with create_temp_file() as signature_path:
            calc_signature(session, node.local_fspath, new_key, signature_path)
            file_transfer.upload_metadata(session, signature_path, new_key, "signature")

        history.add_entry(NodeHistoryEntry.create_delta_only(
            new_key, node.calc_etag(), delta_size
        ))
    else:
        with create_temp_file() as signature_path:
            calc_signature(session, node.local_fspath, new_key, signature_path)
            file_transfer.upload_metadata(session, signature_path, new_key, "signature")

        version = file_transfer.upload_to_root(session, node)

        history = NodeHistory(key=node.key, path=node.path, entries=[])
        history.add_entry(NodeHistoryEntry.create_base_only(
            new_key, node.calc_etag(), version, node.size
        ))
        remote_history = RemoteNodeHistory(history=history, key=node.key, etag=None)

    remote_history.save(session)

    stored_history = StoredNodeHistory.get_or_none(StoredNodeHistory.key == history.key)
    if stored_history is not None:
        stored_history.data = history.dict()
        stored_history.remote_history_etag = remote_history.etag
        stored_history.local_modified_time = node.created_time
        stored_history.local_created_time = node.modified_time
        stored_history.save()
    else:
        StoredNodeHistory.create(
            key=remote_history.key,
            root_folder=RootFolder.for_session(session),
            data=cast(NodeHistory, remote_history.history).dict(),
            local_modified_time=node.created_time,
            local_created_time=node.modified_time,
            remote_history_etag=remote_history.etag
        )

    return SyncActionResult()


@action
def download(
    remote_history: RemoteNodeHistory,
    stored_history: Optional[StoredNodeHistory],
    session: Session,
) -> SyncActionResult:
    """
    1. Without local history
      - Find latest base
      - Download latest base
      - Store history in local DB
    2. With local history
      - Diff remote and local history and find shortest path
      - Fetch deltas one by one patch
      - Store history in local DB
    """
    history = cast(NodeHistory, remote_history.history)
    if stored_history is not None:
        entries, is_absolute = history.diff(stored_history.history)
        if is_absolute:
            local_path = file_transfer.download_to_root(
                session, history.path, entries[0].base_version
            )
            entries = entries[1:]
        else:
            local_path = session.root_folder.path / history.path
        if entries:
            patch_file(session, os.fspath(local_path), [e.key for e in entries])
        local_node = LocalNode.create(local_path, session)
        stored_history.data = history.dict()  # type: ignore
        stored_history.local_modified_time = local_node.created_time
        stored_history.local_created_time = local_node.modified_time
        stored_history.remote_history_etag = remote_history.etag
    else:
        entries, is_absolute = history.diff(None)
        local_path = file_transfer.download_to_root(
            session, history.path, entries[0].base_version
        )
        if entries[1:]:
            patch_file(session, os.fspath(local_path), [e.key for e in entries[1:]])
        local_node = LocalNode.create(local_path, session)
        stored_history = StoredNodeHistory(
            key=remote_history.key,
            root_folder=RootFolder.for_session(session),
            data=history.dict(),
            local_modified_time=local_node.created_time,
            local_created_time=local_node.modified_time,
            remote_history_etag=remote_history.etag
        )

    last_entry = entries[-1]
    file_transfer.download_metadata(
        session, last_entry.key, "signature",
        os.fspath(session.signature_folder / last_entry.key)
    )

    stored_history.save()
    return SyncActionResult()


@action
def delete_local(
    node: LocalNode, stored_history: StoredNodeHistory, session: Session
) -> SyncActionResult:
    (session.signature_folder / stored_history.history.last.key).unlink()
    (node.root_folder / node.path).unlink()
    stored_history.delete().execute()
    return SyncActionResult()


@action
def delete_remote(
    remote_history: RemoteNodeHistory,
    stored_history: StoredNodeHistory,
    session: Session,
) -> SyncActionResult:
    history = cast(NodeHistory, remote_history.history)
    (session.signature_folder / history.last.key).unlink()
    s3util.delete_file(
        session.s3_client, session.storage_bucket,
        f"{session.s3_prefix}/{history.path}"
    )
    history.add_delete_marker()
    remote_history.save(session)
    stored_history.delete().execute()
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
        data=cast(NodeHistory, remote_history.history).dict(),
        local_modified_time=node.created_time,
        local_created_time=node.modified_time,
        remote_history_etag=remote_history.etag
    )
    return SyncActionResult()


@action
def delete_history(stored_history: StoredNodeHistory, session: Session) -> SyncActionResult:
    stored_history.delete().execute()
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
