from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from s3rsync.history import NodeHistory
from s3rsync.models import StoredNodeHistory
from s3rsync.session import Session
from s3rsync.util.file import hash_path, file_checksum


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
    etag: Optional[str]

    def __post_init__(self):
        self.key = hash_path(self.path)

    @classmethod
    def create(cls, local_path: Path, session: Session) -> LocalNode:
        root_folder = session.root_folder.path
        stat = local_path.stat()
        return LocalNode(
            root_folder=root_folder,
            path=local_path.relative_to(root_folder).as_posix(),
            modified_time=int(stat.st_mtime),
            created_time=int(stat.st_ctime),
            size=stat.st_size,
            etag=None
        )

    def updated(self, stored: StoredNodeHistory) -> bool:
        return (
            self.modified_time != stored.local_modified_time
            or self.created_time != stored.local_created_time
        )

    @property
    def local_path(self) -> Path:
        return self.root_folder / self.path

    @property
    def local_fspath(self) -> str:
        return os.fspath(self.local_path)

    def calc_etag(self) -> str:
        if self.etag is None:
            self.etag = file_checksum(self.local_fspath) or ""
        return self.etag
