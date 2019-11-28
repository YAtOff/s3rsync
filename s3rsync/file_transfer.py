from pathlib import Path
import shutil

from s3rsync.session import Session
from s3rsync.node import LocalNode
from s3rsync.util.file import create_temp_file
from s3rsync import s3util


def download_to_root(session: Session, path: str, version: str = None) -> Path:
    with create_temp_file() as tmp_path:
        s3util.download_file(
            session.s3_client,
            session.storage_bucket,
            f"{session.s3_prefix}/{path}",
            tmp_path,
            version=version
        )
        local_path = session.root_folder.path / path
        if not local_path.parent.exists():
            local_path.parent.mkdir(parents=True)
        shutil.move(tmp_path, local_path)
        return local_path


def download_metadata(session: Session, key: str, name: str, local_path: str):
    s3util.download_file(
        session.s3_client,
        session.internal_bucket,
        f"{session.s3_prefix}/{session.sync_metadata_prefix}/entries/{key}/{name}",
        local_path
    )


def upload_to_root(session: Session, node: LocalNode):
    with create_temp_file() as tmp_path:
        shutil.copyfile(node.local_path, tmp_path)
        s3_path = f"{session.s3_prefix}/{node.path}"
        s3util.upload_file(
            session.s3_client, tmp_path, session.storage_bucket, s3_path
        )
        obj = s3util.get_file_metadata(session.s3_client, session.storage_bucket, s3_path)
        return obj["VersionId"]


def upload_metadata(session: Session, local_path: str, key: str, name: str):
    with create_temp_file() as tmp_path:
        shutil.copyfile(local_path, tmp_path)
        s3util.upload_file(
            session.s3_client,
            tmp_path,
            session.internal_bucket,
            f"{session.s3_prefix}/{session.sync_metadata_prefix}/entries/{key}/{name}"
        )
