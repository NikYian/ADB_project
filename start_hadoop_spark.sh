#!/bin/bash

# Start HDFS
echo "Starting HDFS..."
start-dfs.sh

# Start YARN
echo "Starting YARN..."
start-yarn.sh

# Start Spark 
echo "Starting Spark..."
$SPARK_HOME/sbin/start-history-server.sh

echo "HDFS, YARN, and Spark started successfully."
