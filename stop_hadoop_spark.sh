# Stop Spark history server
echo "Stopping Spark history server..."
$SPARK_HOME/sbin/stop-history-server.sh

# Stop Spark
echo "Stopping Spark..."
$SPARK_HOME/sbin/stop-all.sh

# Stop YARN
echo "Stopping YARN..."
stop-yarn.sh

# Stop HDFS
echo "Stopping HDFS..."
stop-dfs.sh

echo "HDFS, YARN, and Spark stopped successfully."