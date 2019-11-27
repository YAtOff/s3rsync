from pathlib import Path
import random
import string


import pytest
from faker import Faker, providers

from s3rsync.history import RemoteNodeHistory
from s3rsync.models import StoredNodeHistory, RootFolder
from s3rsync.node import LocalNode
from s3rsync.sync_action import (
    SyncAction,
    conflict,
    delete_history,
    delete_local,
    delete_remote,
    download,
    nop,
    save_history,
    upload,
)
from s3rsync.sync_logic import handle_node
from s3rsync.util.file import hash_path


class Bunch:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


fake = Faker()
fake.add_provider(providers.internet)
fake.add_provider(providers.date_time)
fake.add_provider(providers.file)


def generate_path():
    return fake.file_path().strip("/")


def generate_key():
    return "".join([random.choice("abcdef0123456789") for i in range(32)])


def generate_etag():
    return "".join([random.choice("abcdef0123456789") for i in range(32)])


def generate_datetime():
    return fake.date_time()


def generate_timestamp():
    return random.randint(1, 1000000000)


def generate_size():
    return random.randint(1, 1000000000)


class Generator:
    def __init__(self, factory):
        self.factory = factory
        self.value = factory()

    def same(self):
        return self.value

    def new(self):
        self.value = self.factory()
        return self.value


path = Generator(generate_path)
key = Generator(generate_key)
etag = Generator(generate_etag)
modified_time = Generator(generate_timestamp)
created_time = Generator(generate_timestamp)


class FileGenerator:
    gen = Bunch(
        path=Generator(generate_path),
        etag=Generator(generate_etag),
        modified_time=Generator(generate_timestamp),
        created_time=Generator(generate_timestamp),
        size=Generator(generate_size),
    )

    def remote(self, deleted=False, **extra_attrs):
        attrs = {
            "key": self.key,
            "etag": self.history_etag,
            "history": Bunch(etag=self.etag, deleted=deleted),
            **extra_attrs,
        }
        return RemoteNodeHistory(**attrs)

    def local(self, **extra_attrs):
        attrs = {
            "root_folder": Path(self.root_folder.path),
            "path": self.path,
            "modified_time": self.modified_time,
            "created_time": self.created_time,
            "size": self.size,
            "etag": self.etag,
            **extra_attrs,
        }
        return LocalNode(**attrs)

    def stored(self, **extra_attrs):
        attrs = {
            "key": self.key,
            "root_folder": self.root_folder,
            "local_modified_time": self.modified_time,
            "local_created_time": self.created_time,
            "remote_history_etag": self.history_etag,
            **extra_attrs,
        }
        return StoredNodeHistory(**attrs)

    def new(self):
        self.path = self.gen.path.new()
        self.key = hash_path(self.path)
        self.etag = self.gen.etag.new()
        self.history_etag = self.gen.etag.new()
        self.modified_time = self.gen.modified_time.new()
        self.created_time = self.gen.created_time.new()
        self.size = self.gen.size.new()
        return self

    root_folder = RootFolder(path="/local")

    @property
    def base_attrs(self):
        return {"key": self.key}


file = FileGenerator()


@pytest.mark.parametrize(
    "number,remote,local,stored,expected_action_factory",
    [
        (1, None, None, None, lambda r, l, s: nop()),
        (
            2, None, None, file.new().stored(),
            lambda r, l, s: delete_history(s)
        ),
        (
            3, None, file.new().local(), None,
            lambda r, l, s: upload(r, l)
        ),
        (
            4, None, file.new().local(), file.stored(),
            lambda r, l, s: delete_local(l, s)
        ),
        (
            5, file.new().remote(), None, None,
            lambda r, l, s: download(r, s)
        ),
        (
            6, file.new().remote(), None, file.stored(),
            lambda r, l, s: delete_remote(r, s)
        ),
        (
            7, file.new().remote(), file.local(), None,
            lambda r, l, s: save_history(r, l)
        ),
        (
            8, file.new().remote(), file.local(etag=etag.new()), None,
            lambda r, l, s: conflict(r, l, s)
        ),
        (
            9, file.new().remote(), file.local(), file.stored(),
            lambda r, l, s: nop()
        ),
        (
            10, file.new().remote(), file.local(modified_time=modified_time.new()), file.stored(),
            lambda r, l, s: upload(r, l)
        ),
        (
            11, file.new().remote(etag=etag.new()), file.local(), file.stored(),
            lambda r, l, s: download(r, s)
        ),
        (
            12,
            file.new().remote(etag=etag.new()),
            file.local(modified_time=modified_time.new()),
            file.stored(),
            lambda r, l, s: nop()
        ),
        (
            13,
            file.new().remote(etag=etag.new()),
            file.local(modified_time=modified_time.new(), etag=etag.new()),
            file.stored(),
            lambda r, l, s: conflict(r, l, s)
        ),
        (
            14,
            file.new().remote(deleted=True),
            None,
            file.stored(),
            lambda r, l, s: delete_history(s)
        ),
        (
            15,
            file.new().remote(deleted=True),
            file.local(),
            None,
            lambda r, l, s: delete_local(l, s)
        ),
        (
            16,
            file.new().remote(deleted=True),
            file.local(),
            file.stored(),
            lambda r, l, s: delete_local(l, s)
        ),
        (
            17,
            file.new().remote(deleted=True),
            file.local(modified_time=modified_time.new()),
            file.stored(),
            lambda r, l, s: conflict(r, l, s)
        ),
    ]
)
def test_handle_node(number, remote, local, stored, expected_action_factory):
    action = handle_node(remote, local, stored)
    expected_action = expected_action_factory(remote, local, stored)
    assert repr(action) == repr(expected_action)
