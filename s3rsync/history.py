from __future__ import annotations

import json
import os
from dataclasses import dataclass
from io import BytesIO
from typing import List, Tuple, Optional, cast
from uuid import uuid4

from pydantic import BaseModel

from s3rsync.exceptions import MissingNodeHistoryEntryError
from s3rsync.s3util import download_to_fd, get_file_metadata, upload_from_fd
from s3rsync.session import Session
from s3rsync.util.file import hash_path


class NodeHistoryEntry(BaseModel):
    key: str
    deleted: bool
    etag: Optional[str]
    base_version: Optional[str]
    base_size: int
    has_delta: bool
    delta_size: int

    @classmethod
    def generate_key(cls) -> str:
        return uuid4().hex

    @classmethod
    def create_deleted(cls) -> NodeHistoryEntry:
        return cls(
            key=cls.generate_key(),
            deleted=True,
            etag=None,
            base_version=None,
            base_size=0,
            has_delta=False,
            delta_size=0
        )

    @classmethod
    def create_delta_only(cls, key: str, etag: str, delta_size: int) -> NodeHistoryEntry:
        return cls(
            key=key,
            deleted=False,
            etag=etag,
            base_version=None,
            base_size=0,
            has_delta=True,
            delta_size=delta_size
        )

    @classmethod
    def create_base_only(cls, key: str, etag: str, base_version: str, base_size: int) -> NodeHistoryEntry:
        return cls(
            key=key,
            deleted=False,
            etag=etag,
            base_version=base_version,
            base_size=base_size,
            has_delta=False,
            delta_size=0
        )


class NodeHistory(BaseModel):
    path: str
    key: str
    entries: List[NodeHistoryEntry]

    @classmethod
    def create(cls, path: str, entries: List[NodeHistoryEntry] = None) -> NodeHistory:
        return cls(
            path=path,
            key=hash_path(path),
            entries=entries or []
        )

    def diff(self, other: Optional[NodeHistory]) -> Tuple[List[NodeHistoryEntry], bool]:
        is_absolute = False
        result = []
        if other is None:
            is_absolute = True
            for entry in reversed(self.entries):
                result.append(entry)
                if entry.base_version:
                    break
        else:
            last_key = other.last.key
            delta_size = 0
            last_base = None
            for entry in reversed(self.entries):
                if entry.deleted or entry.key == last_key:
                    break
                elif not entry.has_delta:
                    result.append(entry)
                    is_absolute = True
                    break
                else:
                    delta_size += entry.delta_size
                    if entry.base_version and not last_base:
                        last_base = len(result), entry.base_size
                    if last_base and delta_size > last_base[1]:
                        result = result[:last_base[0] + 1]
                        is_absolute = True
                        break
                result.append(entry)

        return list(reversed(result)), is_absolute

    def add_delete_marker(self) -> None:
        self.entries.append(NodeHistoryEntry.create_deleted())

    def add_entry(self, entry: NodeHistoryEntry) -> None:
        self.entries.append(entry)

    @property
    def deleted(self) -> bool:
        return self.last.deleted

    @property
    def etag(self) -> Optional[str]:
        return self.last.etag

    @property
    def last(self) -> NodeHistoryEntry:
        if not self.entries or self.entries[-1].deleted:
            raise MissingNodeHistoryEntryError
        return self.entries[-1]


@dataclass
class RemoteNodeHistory:
    history: Optional[NodeHistory]
    key: str
    etag: Optional[str]

    @classmethod
    def from_s3_object(cls, obj) -> RemoteNodeHistory:
        key = obj["Key"].rpartition("/")[-1].partition(".")[0]
        return cls(
            history=None,
            key=key,
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
            f"{session.s3_prefix}/{session.sync_metadata_prefix}/history/{self.key}",
            fd,
        )
        fd.seek(0, os.SEEK_SET)
        data = json.load(fd)
        self.history = NodeHistory.parse_obj(data)

    def save(self, session: Session) -> None:
        if not self.is_loaded:
            return
        fd = BytesIO(
            json.dumps(cast(NodeHistory, self.history).dict()).encode("utf-8")
        )
        fd.seek(0, os.SEEK_SET)
        s3_path = f"{session.s3_prefix}/{session.sync_metadata_prefix}/history/{self.key}"
        upload_from_fd(session.s3_client, fd, session.internal_bucket, s3_path)
        obj = get_file_metadata(session.s3_client, session.internal_bucket, s3_path)
        self.modified_time = obj["LastModified"],
        self.etag = obj.get("ETag", "").strip('"')

    def updated(self, stored) -> bool:
        return self.etag != stored.remote_history_etag

    @property
    def deleted(self) -> bool:
        return self.history and self.history.deleted  # type: ignore

    @property
    def exists(self) -> bool:
        return not self.deleted
