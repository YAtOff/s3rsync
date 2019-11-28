#!/usr/bin/env python

from pathlib import Path
import shutil

import click
import boto3  # type: ignore
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


@click.command()
@click.argument("s3_prefix")
@click.argument("root_folder")
def main(s3_prefix, root_folder):
    if Path(root_folder).exists():
        shutil.rmtree(root_folder)
    Path(root_folder).mkdir()

    clear_s3_prefix(settings.STORAGE_BUCKET, s3_prefix)
    clear_s3_prefix(settings.INTERNAL_BUCKET, f"{s3_prefix}/{settings.SYNC_METADATA_PREFIX}")

    if Path("db").exists():
        shutil.rmtree("db")
    Path("db").mkdir()


if __name__ == "__main__":
    main()
