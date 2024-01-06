# Import necessary Spark modules
from pyspark.sql import SparkSession
import datetime
import csv

# Create a Spark session with specified configurations
spark = SparkSession \
    .builder \
    .appName("Q2_RDD") \
    .getOrCreate() \
    .sparkContext
  
# Load the first CSV file into an RDD
rdd1 = spark.textFile("hdfs://okeanos-master:54310/user/project/Crime_Data_from_2010_to_2019.csv") \
    .map(lambda x: next(csv.reader([x])))
    
header1 = rdd1.first()
rdd1 = rdd1.filter(lambda row: row != header1)

# Load the second CSV file into an RDD
rdd2 = spark.textFile("hdfs://okeanos-master:54310/user/project/Crime_Data_from_2020_to_Present.csv") \
    .map(lambda x: next(csv.reader([x])))
  
header2 = rdd2.first()  
rdd2 = rdd2.filter(lambda row: row != header2)

# Merge the two RDDs
rdd = rdd1.union(rdd2)
rdd = rdd.map(lambda col: (col[3], col[14]))

filtered_rdd = rdd.filter(lambda row: (row[1] == '101') or (row[1] == 101) )

def get_interval(time_occ):
    # Convert the time_occ to a datetime object for easier comparison
    time_object = datetime.datetime.strptime(time_occ, "%H%M")

    if datetime.time(5, 0) <= time_object.time() < datetime.time(12, 0):
        return "Morning"
    elif datetime.time(12, 0) <= time_object.time() < datetime.time(17, 0):
        return "Afternoon"
    elif datetime.time(17, 0) <= time_object.time() < datetime.time(21, 0):
        return "Evening"
    elif (datetime.time(21, 0) <= time_object.time()) or  (time_object.time() < datetime.time(5, 0)):
        return "Night"
    
# Map each row to a tuple of (interval, 1)
mapped_rdd = filtered_rdd.map(lambda col: (get_interval(col[0]), 1))

# Reduce by key to sum occurrences within each interval
result_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)

print(result_rdd.collect())

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = result_rdd.map(toCSVLine)
lines.saveAsTextFile('results/q1_RDD.csv')

import subprocess

hdfs_path = "hdfs://okeanos-master:54310/user/user/results/q1_RDD.csv"
local_path = "/home/user/Project/results/"

subprocess.run(["hadoop", "fs", "-copyToLocal", "-f", hdfs_path, local_path])

# Stop the Spark session
spark.stop()

    