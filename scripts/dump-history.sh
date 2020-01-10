#!/bin/bash

dump_history() {
    f=$(mktemp) && \
    aws s3 cp "$rs_history/$1" $f > /dev/null && \
    (cat $f | jq '.') && \
    rm -rf $f
}

export -f dump_history

aws s3 ls "$rs_history/" | awk '{print $(NF)}' | xargs -I{} bash -c 'dump_history "$@"' _ {}
