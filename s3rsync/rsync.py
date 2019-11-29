import os
import shutil
from typing import List, cast

from librsync import patch_from_paths, delta_from_paths, signature_from_paths

from s3rsync.session import Session
from s3rsync.file_transfer import download_metadata
from s3rsync.util.file import create_temp_file

# TODO: error handling for librsync


def calc_signature(session: Session, local_path: str, key: str, signature_path: str) -> None:
    signature_from_paths(local_path, signature_path)
    # TODO: keep only the last signature
    shutil.copy(
        signature_path,
        os.fspath(session.signature_folder / key)
    )


def calc_delta(session: Session, local_path: str, key: str, delta_path: str) -> None:
    with create_temp_file() as tmp_file:
        if (session.signature_folder / key).exists():
            signature_path = os.fspath(session.signature_folder / key)
        else:
            download_metadata(session, key, "signature", tmp_file)
            signature_path = tmp_file

        delta_from_paths(signature_path, local_path, delta_path)


def patch_file(session: Session, local_path: str, keys: List[str]) -> None:
    with create_temp_file() as tmp1, create_temp_file() as tmp2:
        tmp_paths = [tmp1, tmp2]
        base_path = local_path
        result_path = None
        for key in keys:
            result_path = tmp_paths.pop()
            apply_delta(session, base_path, key, result_path)
            if base_path != local_path:
                tmp_paths.append(base_path)
            base_path = result_path
        shutil.move(cast(str, result_path), local_path)


def apply_delta(session: Session, base_path: str, key: str, result_path: str) -> None:
    with create_temp_file() as delta_path:
        download_metadata(session, key, "delta", delta_path)
        patch_from_paths(base_path, delta_path, result_path)
