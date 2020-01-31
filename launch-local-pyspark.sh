#!/bin/bash

spark-submit --verbose --master local \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir="file:/usr/local/ML/Master/Module1/DistributedData/TPs/victorGuerraKMeans/logs" \
    $1