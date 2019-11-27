from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from s3rsync.models import StoredNodeHistory
from s3rsync.history import NodeHistory
from s3rsync.session import Session
from s3rsync.util.file import hash_path


@dataclass
class NodeOptions:
    main_bucket: str
    main_bucket_prefix: str
    rsync_bucket: str
    rsync_bucket_prefix: str


@dataclass
class RemoteNode:
    root: Path
    path: str
    optins: NodeOptions

    @property
    def history(self) -> NodeHistory:
        pass

    @property
    def s3_path(self) -> str:
        return Path(self.path).relative_to(self.root).as_posix()


@dataclass
class LocalNode:
    root_folder: Path
    path: str
    key: str = field(init=False)
    modified_time: float
    created_time: float
    size: int
    etag: str

    def __post_init__(self):
        self.key = hash_path(self.path)

    @classmethod
    def create(cls, local_path: Path, session: Session) -> LocalNode:
        root_folder = session.root_folder.path
        stat = local_path.stat()
        return LocalNode(
            root_folder=root_folder,
            path=local_path.relative_to(root_folder).as_posix(),
            modified_time=stat.st_mtime,
            created_time=stat.st_ctime,
            size=stat.st_size,
            etag="",
        )

    def updated(self, stored: StoredNodeHistory) -> bool:
        return (
            self.modified_time != stored.local_modified_time
            or self.created_time != stored.local_created_time
        )
