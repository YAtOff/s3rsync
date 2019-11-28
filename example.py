from pathlib import Path
from typing import List

import click
from dynaconf import settings  # type: ignore
from faker import Faker, providers
import peewee

from s3rsync.session import Session
from s3rsync.local_db import open_database
from s3rsync.sync_action import upload
from s3rsync.node import LocalNode
from s3rsync import models  # NOQA
from s3rsync.util.misc import all_subclasses


fake = Faker()
fake.add_provider(providers.file)


def create_file(root_folder: Path, size: int) -> Path:
    path = root_folder / fake.file_name()
    with open(path, "wb") as f:
        f.seek(size - 1)
        f.write(b"\0")
    return Path(path)


def MB(val) -> int:
    return val * 1024 * 1024


examples = {}


def example(func):
    examples[func.__name__] = func
    return func


@example
def upload_new(session):
    path = create_file(session.root_folder.path, MB(1))
    action = upload(None, LocalNode.create(path, session))
    action(session)


@example
def upload_again(session, path):
    # 1. get remote history for file
    remote_history = None
    action = upload(remote_history, LocalNode.create(path, session))
    action(session)


@click.command()
@click.argument("example_name")
@click.argument("args", nargs=-1)
def main(example_name: str, args: List[str]):
    s3_prefix = "rsync.test.1"
    root_folder = "root"
    db_exists = Path(settings.LOCAL_DB).exists()
    with open_database(settings.LOCAL_DB) as db:
        if not db_exists:
            db.create_tables(all_subclasses(peewee.Model))
        session = Session.create(s3_prefix, root_folder)
        examples[example_name](session, *args)


if __name__ == "__main__":
    main()
