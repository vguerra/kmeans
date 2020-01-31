#!/bin/bash

export SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS -Dspark.history.fs.logDirectory=file:/usr/local/ML/Master/Module1/DistributedData/TPs/victorGuerraKMeans/logs"
/usr/local/Cellar/apache-spark/2.4.4/libexec/sbin/start-history-server.sh