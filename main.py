import click
from dynaconf import settings  # type: ignore

from s3rsync.session import Session
from s3rsync.sync import SyncWorker
from s3rsync.local_db import open_database


@click.command()
@click.argument("s3_prefix")
@click.argument("root_folder")
@click.option("--once", default=False)
def main(s3_prefix, root_folder, once):
    with open_database(settings.LOCAL_DB):
        worker = SyncWorker(Session(s3_prefix, root_folder))
        if once:
            worker.run_once()
        else:
            worker.run()


if __name__ == "__main__":
    main()
