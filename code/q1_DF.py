from app_duration import AppDuration
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, count, row_number
from pyspark.sql import Window

# Create a Spark session with specified configurations
spark = SparkSession.builder \
        .appName('Q1_DF') \
        .config("spark.master", "yarn") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

# Read the CSV files into DataFrames
df1 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
df2 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)

# Union the two DataFrames
df = df1.union(df2)

# Convert date columns to the appropriate date format
df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("DATE OCC", to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a'))

# Extract year and month from the "Date Rptd" column
date_rptd = df.select('Date Rptd')
date_rptd = date_rptd.withColumn("Year", year("Date Rptd")).withColumn("Month", month("Date Rptd")).drop("Date Rptd")

# Group by year and month, calculate the total number of crimes
crime_total = date_rptd.groupBy("Year", "Month").agg(count("*").alias("crime_total"))

# Define a window specification to partition by the "Year" column and order by the "crime_total" column
window_spec = Window().partitionBy("Year").orderBy(col("crime_total").desc())

# Use the row_number function to assign row numbers within each group
df_sorted = crime_total.withColumn("row_number", row_number().over(window_spec))

# Filter to keep only the top three within each group
df_top_three_DF = df_sorted.filter(col("row_number") <= 3)

# Show the resulting DataFrame
df_top_three_DF.show(df_top_three_DF.count(), truncate=False)

# # Save the DataFrame to a CSV file
# df_top_three_DF \
#   .coalesce(1) \
#   .write \
#   .mode('overwrite') \
#   .option('header', 'true') \
#   .csv('results/q1_DF.csv')

# import subprocess

# hdfs_path = "hdfs://okeanos-master:54310/user/user/results/q1_DF.csv"
# local_path = "/home/user/Project/results/"

# subprocess.run(["hadoop", "fs", "-copyToLocal", "-f", hdfs_path, local_path])

# Stop the Spark session

app_id = spark.sparkContext.applicationId


duration = AppDuration(app_id)

print('Sum duration of Jobs:', duration, ' seconds')


spark.stop()
