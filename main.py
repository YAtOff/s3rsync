import logging

from pathlib import Path
import click
from dynaconf import settings  # type: ignore
import peewee

from s3rsync.session import Session
from s3rsync.sync import SyncWorker
from s3rsync.local_db import open_database
from s3rsync.util.misc import all_subclasses
from s3rsync import models  # NOQA


logging.basicConfig(level=logging.INFO)


@click.command()
@click.argument("s3_prefix")
@click.argument("root_folder")
@click.option("--once/--no-once", default=False)
def main(s3_prefix, root_folder, once):
    db_exists = Path(settings.LOCAL_DB).exists()
    with open_database(settings.LOCAL_DB) as db:
        if not db_exists:
            db.create_tables(all_subclasses(peewee.Model))
        worker = SyncWorker(Session.create(s3_prefix, root_folder))
        if once:
            worker.run_once()
        else:
            worker.run()


if __name__ == "__main__":
    main()
