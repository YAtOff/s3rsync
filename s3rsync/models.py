from __future__ import annotations

import json
from typing import Optional, Dict

import peewee  # type: ignore

from s3rsync.session import Session
from s3rsync.local_db import database
from s3rsync.history import NodeHistory


def recored_as_dict(record):
    if isinstance(record, peewee.Model):
        return {
            f: recored_as_dict(getattr(record, f))
            for f in record._meta.sorted_field_names
        }
    else:
        return record


class JSONField(peewee.TextField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


class RootFolder(peewee.Model):
    path = peewee.CharField()

    class Meta:
        database = database

    @classmethod
    def for_session(cls, session: Session) -> RootFolder:
        return cls.get_or_create(path=session.root_folder.fspath)[0]


class StoredNodeHistory(peewee.Model):
    id = peewee.AutoField()
    key = peewee.CharField(index=True)
    root_folder = peewee.ForeignKeyField(RootFolder, on_delete="CASCADE")
    data = JSONField()
    local_modified_time = peewee.IntegerField()
    local_created_time = peewee.IntegerField()
    remote_history_etag = peewee.DateTimeField()

    class Meta:
        database = database

    _history: Optional[Dict[float, NodeHistory]] = None

    @property
    def history(self) -> NodeHistory:
        if self._history is None:
            self._history = {}
        key = self.remote_history_etag
        if key not in self._history:
            self._history[key] = NodeHistory.parse_obj(self.data)
        return self._history[key]
