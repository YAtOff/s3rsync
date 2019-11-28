from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3  # type: ignore
from dynaconf import settings  # type: ignore


@dataclass
class RootFolder:
    path: Path
    fspath: str

    @classmethod
    def create(cls, fspath: str) -> RootFolder:
        path = Path(fspath).resolve()
        return cls(path=path, fspath=os.fspath(path))


@dataclass
class Session:
    s3_prefix: str
    root_folder: RootFolder
    s3_client: Any
    storage_bucket: str
    internal_bucket: str
    sync_metadata_prefix: str
    signature_folder: Path

    @classmethod
    def create(cls, s3_prefix: str, root_fspath: str) -> Session:
        root_folder = RootFolder.create(root_fspath)
        signature_folder = Path(settings.SIGNATURE_FOLDER)
        if not signature_folder.exists():
            signature_folder.mkdir(parents=True)

        return cls(
            root_folder=root_folder,
            s3_prefix=s3_prefix,
            s3_client=boto3.client("s3"),
            storage_bucket=settings.STORAGE_BUCKET,
            internal_bucket=settings.INTERNAL_BUCKET,
            sync_metadata_prefix=settings.SYNC_METADATA_PREFIX,
            signature_folder=signature_folder
        )
