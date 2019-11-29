#!/usr/bin/env python

import shutil
from pathlib import Path
from functools import partial

import boto3  # type: ignore
import click
from dynaconf import settings  # type: ignore


s3_client = boto3.client("s3")


def list_versions(bucket, prefix, count):
    return s3_client.list_object_versions(
        Bucket=bucket, Prefix=prefix.rstrip("/") + "/", MaxKeys=count
    ).get("Versions", [])


def clear_s3_prefix(bucket, prefix):
    versions = list_versions(bucket, prefix, 1000)
    while versions:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [
                    {"Key": v["Key"], "VersionId": v["VersionId"]} for v in versions
                ],
                "Quiet": False,
            },
        )
        versions = list_versions(bucket, prefix, 1000)


def clear_remote(s3_prefix):
    clear_s3_prefix(settings.STORAGE_BUCKET, s3_prefix)
    clear_s3_prefix(settings.INTERNAL_BUCKET, f"{s3_prefix}/{settings.SYNC_METADATA_PREFIX}")


def clear_local(root_folder):
    if Path(root_folder).exists():
        shutil.rmtree(root_folder)
    Path(root_folder).mkdir()

    paths = [
        Path(settings.LOCAL_DB),
        Path(settings.SIGNATURE_FOLDER)
    ]
    for path in paths:
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()


@click.command()
@click.argument("target")
@click.argument("s3_prefix")
@click.argument("root_folder")
def main(target, s3_prefix, root_folder):
    actions = {
        "all": (partial(clear_remote, s3_prefix), partial(clear_local, root_folder)),
        "remote": (partial(clear_remote, s3_prefix),),
        "local": (partial(clear_local, root_folder),),
    }[target]
    for action in actions:
        action()


if __name__ == "__main__":
    main()
