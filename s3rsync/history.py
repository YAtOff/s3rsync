from __future__ import annotations

import json
import os
from dataclasses import dataclass
from io import BytesIO
from typing import Iterable, List, Optional

from pydantic import BaseModel

from s3rsync.s3util import download_to_fd, upload_from_fd, get_file_metadata
from s3rsync.session import Session


"""
pydantic:
    BaseMode.parse_obj(dict) -> instance
    BaseMode#dict() -> dict
"""


class NodeHistoryEntry(BaseModel):
    id: str
    base_version: Optional[str]
    base_hash: Optional[str]
    base_size: int
    has_delta: bool
    delta_size: int


class NodeHistory(BaseModel):
    path: str
    etag: str
    deleted: bool
    base_path: str
    entries: List[NodeHistoryEntry]

    def diff(self, history: NodeHistory) -> Iterable[NodeHistoryEntry]:
        pass

    def add_delete_marker(self) -> None:
        pass


@dataclass
class RemoteNodeHistory:
    history: Optional[NodeHistory]
    key: str
    etag: str

    @classmethod
    def from_s3_object(cls, obj) -> RemoteNodeHistory:
        key = obj["Key"].rpartition("/")[-1].partition(".")[0]
        return cls(
            history=None,
            key=key,
            modified_time=obj["LastModified"],
            etag=obj.get("ETag", "").strip('"'),
        )

    @property
    def is_loaded(self) -> bool:
        return self.history is not None

    def load(self, session: Session) -> None:
        fd = BytesIO()
        download_to_fd(
            session.s3_client,
            session.internal_bucket,
            f"{session.sync_metadata_prefix}/{session.s3_prefix}/history/{self.key}",
            fd,
        )
        fd.seek(0, os.SEEK_SET)
        data = json.load(fd)
        self.history = NodeHistory.parse_obj(data)

    def save(self, session: Session) -> None:
        fd = BytesIO()
        json.dump(self.history.dict(), fd)
        fd.seek(0, os.SEEK_SET)
        s3_path = f"{session.sync_metadata_prefix}/{session.s3_prefix}/history/{self.key}"
        upload_from_fd(session.s3_client, fd, session.internal_bucket, s3_path)
        obj = get_file_metadata(session.s3_client, session.internal_bucket, s3_path)
        self.modified_time = obj["LastModified"],
        self.etag = obj.get("ETag", "").strip('"'),

    def updated(self, stored) -> bool:
        return self.etag != stored.remote_history_etag

    @property
    def deleted(self) -> bool:
        return self.history.deleted

    @property
    def exists(self) -> bool:
        return not self.deleted
