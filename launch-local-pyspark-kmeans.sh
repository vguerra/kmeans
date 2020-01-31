#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

spark-submit --verbose --master local \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir="file:/usr/local/ML/Master/Module1/DistributedData/TPs/victorGuerraKMeans/logs" \
    "$DIR/kmeans-vguerra.py" $1 $2 $3 $4 $5