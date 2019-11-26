CHUNK_SIZE = 1000


def list_versions(client, bucket, prefix):
    key_marker = None
    version_id_marker = None
    has_more_items = True

    while has_more_items:
        extra_kwargs = (
            {"KeyMarker": key_marker, "VersionIdMarker": version_id_marker}
            if key_marker
            else {}
        )
        result = client.list_object_versions(
            Bucket=bucket,
            Prefix=prefix.rstrip("/") + "/",
            MaxKeys=CHUNK_SIZE,
            **extra_kwargs
        )
        has_more_items = result["IsTruncated"]
        key_marker = result["KeyMarker"]
        version_id_marker = result["VersionIdMarker"]

        yield from (v for v in result.get("Versions", []) if v["IsLatest"])


def get_file_metadata(client, bucket, s3_path):
    obj = client.head_object(Bucket=bucket, Key=s3_path)
    obj["Key"] = s3_path
    return obj


def upload_file(client, local_path, bucket, s3_path):
    client.upload_file(local_path, bucket, s3_path)


def upload_from_fd(client, fd, bucket, s3_path):
    client.upload_fileobj(fd, bucket, s3_path)


def download_file(client, bucket, s3_path, local_path):
    client.download_file(bucket, s3_path, local_path)


def download_to_fd(client, bucket, s3_path, fd):
    client.download_fileobj(bucket, s3_path, fd)


def delete_file(client, bucket, s3_path):
    client.delete_object(bucket, s3_path)


def show_versions(bucket, prefix):
    from pprint import pprint

    import boto3  # type: ignore

    s3 = boto3.client("s3")
    for v in list_versions(s3, bucket, prefix):
        user_prefix, _, path = v["Key"].partition("/")
        pprint(
            dict(
                user_prefix=user_prefix,
                timestamp=v["LastModified"].isoformat()[:-6] + ".000Z",
                path=path,
                version_id=v["VersionId"],
                size=int(v["Size"]),
                etag=v["ETag"].strip('"'),
            )
        )
