## Start and Stop Hadoop, Yarn, Spark, and Spark History Server

To start Hadoop, Yarn, Spark, and Spark History Server:

```start_hadoop_spark.sh```

To stop Hadoop, Yarn, Spark, and Spark History Server:

```stop_hadoop_spark.sh```

## Conda Environment

To create the conda environment:

```conda env create -f pyspark_conda_env.tar.gz```

To activate the conda environment:

```conda activate pyspark_conda_env```

## Query 1 

To run Query 1 with the Dataframe API:

```python code/q1_DF.py```

To run Query 1 with the SQL API:

```python code/q1_SQL.py```

## Query 2 

To run Query 2 with the Dataframe API:

```python code/q2_DF.py```

To run Query 2 with the RDD API:

```python code/q2_RDD.py```

## Query 3 

*To change the number of executors change NUM_EXECUTORS*

To run Query 3:

```python code/q3.py```

## Query 4 

To run Query 4:

```python code/q4.py```

## Useful Commands 

Turn off safe mode 

``` hdfs dfsadmin -safemode leave```

HDFS Report

 ```hdfs dfsadmin -report```

See size information about HDFS filesystem
 ```hdfs dfs -du -h /user```

Remove Staging Log and event log
```hdfs dfs -rm -r -skipTrash /user/user/.sparkStaging```
```hdfs dfs -rm -r -skipTrash /spark.eventLog/*```
