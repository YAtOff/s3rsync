#!/bin/bash

. env

s3ls "$rs_history/" | awk '{print $(NF)}' | xargs -I{} aws s3 cp "$rs_history/{}" /dev/stdout

