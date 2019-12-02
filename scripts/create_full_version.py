import logging

from pathlib import Path
import click
from dynaconf import settings  # type: ignore
import peewee

from s3rsync.session import Session
from s3rsync.local_db import open_database
from s3rsync.util.misc import all_subclasses
from s3rsync import models  # NOQA
from s3rsync.node import LocalNode
from s3rsync.history import RemoteNodeHistory
from s3rsync.rsync import patch_file
from s3rsync.s3util import download_file, upload_file, get_file_metadata
from s3rsync.util.file import create_temp_file


logging.basicConfig(level=logging.INFO)


def create_full_version(session: Session, local_path: str):
    node = LocalNode.create(Path(local_path).resolve(), session)
    remote_history = RemoteNodeHistory(history=None, key=node.key, etag=None)
    remote_history.load(session)

    history = remote_history.history
    last = history.last
    prev = history.entries[-2]

    assert last.base_version is None
    assert last.has_delta
    assert prev.base_version is not None

    with create_temp_file() as base_path:
        s3_path = f"{session.s3_prefix}/{node.path}"
        download_file(
            session.s3_client, session.storage_bucket, s3_path, base_path,
            version=prev.base_version
        )
        patch_file(session, base_path, [last.key])
        upload_file(
            session.s3_client, base_path, session.storage_bucket, s3_path,
        )
        obj = get_file_metadata(session.s3_client, session.storage_bucket, s3_path)
        last.base_version = obj["VersionId"]
        last.base_size = int(obj.get("Size", 0))
        remote_history.save(session)


@click.command()
@click.argument("s3_prefix")
@click.argument("root_folder")
@click.argument("path")
def main(s3_prefix: str, root_folder: str, path: str):
    db_exists = Path(settings.LOCAL_DB).exists()
    with open_database(settings.LOCAL_DB) as db:
        if not db_exists:
            db.create_tables(all_subclasses(peewee.Model))
        session = Session.create(s3_prefix, root_folder)
        create_full_version(session, path)


if __name__ == "__main__":
    main()
