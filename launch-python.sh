#!/bin/bash

spark-submit --master yarn \
    --deploy-mode cluster \
    --executor-cores 4 \
    --num-executors 11 \
    --executor-memory 5g \
    --conf spark.yarn.executor.memoryOverheead=2g \
    --conf spark.driver.memory=5g \
    --conf spark.driver.cores=1 \
    --conf spark.yarn.jars="file:///home/cluster/shared/spark/jars/*.jar" \
    kmeans-vguerra.py