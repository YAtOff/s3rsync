import logging
import json
from base64 import b64decode

import boto3

s3_client = boto3.client("s3")


def handle_event(event):
    bucket = event["s3"]["bucket"]["name"]
    if not bucket.endswith("-internal"):
        return 1

    obj = event["s3"]["object"]
    user_prefix, _, path = obj["key"].partition("/")
    if not path.startswith("rsync/history"):
        return 1

    logging.info("%s / %s -> ", user_prefix, path)

    return 1


def event_from_record(record):
    return json.loads(
        b64decode(record["kinesis"]["data"]).decode("utf-8")
    )


def lambda_handler(event, context):
    handled = 0
    for record in event["Records"]:
        try:
            event = event_from_record(record)
            if event["event_type"] == "s3":
                handled += handle_event(event["data"])
        except Exception:
            logging.exception("Error handling record")

    return handled
