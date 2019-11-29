import logging
from pathlib import Path
from pprint import pprint
from typing import List

import click
import peewee
from dynaconf import settings  # type: ignore
from faker import Faker, providers

from s3rsync.history import RemoteNodeHistory
from s3rsync.local_db import open_database
from s3rsync.node import LocalNode
from s3rsync.session import Session
from s3rsync.sync_action import delete_local, download, upload, delete_remote
from s3rsync.util.log import print_line
from s3rsync.util.misc import all_subclasses
from s3rsync import models


logging.basicConfig(level=logging.INFO)


fake = Faker()
fake.add_provider(providers.file)


def create_file(root_folder: Path, size: int, filename=None) -> Path:
    path = root_folder / (filename or fake.file_name())
    with open(path, "wb") as f:
        f.seek(size - 1)
        f.write(b"\0")
    return Path(path)


def MB(val) -> int:
    return val * 1024 * 1024


def show_history(session: Session, key: str):
    print_line("History")
    print_line("Remote")
    remote_history = RemoteNodeHistory(key=key, etag=None, history=None)
    remote_history.load(session)
    pprint(remote_history.history.dict())
    stored_history = models.StoredNodeHistory\
        .get_or_none(models.StoredNodeHistory.key == key)
    print_line("Stored")
    pprint(models.recored_as_dict(stored_history) if stored_history else None)
    print_line("End History")


examples = {}


def example(func):
    examples[func.__name__] = func
    return func


@example
def upload_new(session: Session, filename=None):
    path = create_file(session.root_folder.path, MB(1), filename=filename)
    node = LocalNode.create(path, session)
    action = upload(None, node)
    action(session)
    show_history(session, node.key)


@example
def upload_again(session: Session, path: str):
    path = Path(path).resolve()
    with open(path, "ab") as f:
        f.write(b"1")
    node = LocalNode.create(path, session)
    remote_history = RemoteNodeHistory(key=node.key, etag=None, history=None)
    remote_history.load(session)
    upload(remote_history, node)(session)
    show_history(session, node.key)


@example
def download_first_time(session: Session):
    # upload
    path = create_file(session.root_folder.path, MB(1))
    node = LocalNode.create(path, session)
    upload(None, node)(session)

    # clear local
    delete_local(
        node,
        models.StoredNodeHistory.get(models.StoredNodeHistory.key == node.key)
    )(session)

    # download
    remote_history = RemoteNodeHistory(key=node.key, etag=None, history=None)
    remote_history.load(session)
    download(remote_history, None)(session)

    show_history(session, node.key)


@example
def download_new_version(session: Session, path: str):
    path = Path(path).resolve()
    node = LocalNode.create(path, session)
    remote_history = RemoteNodeHistory(key=node.key, etag=None, history=None)
    remote_history.load(session)
    stored_history = models.StoredNodeHistory\
        .get(models.StoredNodeHistory.key == node.key)
    download(remote_history, stored_history)(session)

    show_history(session, node.key)


@example
def clear_remote(session: Session):
    # upload
    path = create_file(session.root_folder.path, MB(1))
    node = LocalNode.create(path, session)
    upload(None, node)(session)

    remote_history = RemoteNodeHistory(key=node.key, etag=None, history=None)
    remote_history.load(session)
    stored_history = models.StoredNodeHistory\
        .get(models.StoredNodeHistory.key == node.key)

    delete_remote(remote_history, stored_history)(session)
    node.local_path.unlink()

    show_history(session, node.key)


@click.command()
@click.option("--root", default="root")
@click.option("--s3prefix", default="rsync.test.1")
@click.argument("example_name")
@click.argument("args", nargs=-1)
def main(root: str, s3prefix: str, example_name: str, args: List[str]):
    if example_name == "list":
        for example in examples:
            print(example)
        return
    db_exists = Path(settings.LOCAL_DB).exists()
    with open_database(settings.LOCAL_DB) as db:
        if not db_exists:
            db.create_tables(all_subclasses(peewee.Model))
        session = Session.create(s3prefix, root)
        examples[example_name](session, *args)


if __name__ == "__main__":
    main()
