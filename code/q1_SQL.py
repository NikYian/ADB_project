from app_duration import AppDuration
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count

# Create a Spark session with specified configurations
spark = SparkSession.builder \
        .appName('Q1_SQL') \
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

# Register the DataFrame as a temporary SQL table
df.createOrReplaceTempView("crime_data")

# Write the SQL query to calculate the top three crime counts for each year and month
sql_query = """
    SELECT Year, Month, crime_total, row_number
    FROM (
        SELECT Year, Month, crime_total,
               ROW_NUMBER() OVER (PARTITION BY Year ORDER BY crime_total DESC) AS row_number
        FROM (
            SELECT YEAR(`Date Rptd`) AS Year, MONTH(`Date Rptd`) AS Month, COUNT(*) AS crime_total
            FROM crime_data
            GROUP BY Year, Month
        ) tmp
    ) tmp2
    WHERE row_number <= 3
"""

# Execute the SQL query
df_top_three_sql = spark.sql(sql_query)

# Get the count of rows in the result
count = df_top_three_sql.count()

# Show the result
df_top_three_sql.show(count, truncate=False)


# # Save the DataFrame to a CSV file
# df_top_three_sql \
#   .coalesce(1) \
#   .write \
#   .mode('overwrite') \
#   .option('header', 'true') \
#   .csv('results/q1_SQL.csv')

# import subprocess

# hdfs_path = "hdfs://okeanos-master:54310/user/user/results/q1_SQL.csv"
# local_path = "/home/user/Project/results/"

# subprocess.run(["hadoop", "fs", "-copyToLocal", "-f", hdfs_path, local_path])

# Stop the Spark session

app_id = spark.sparkContext.applicationId


duration = AppDuration(app_id)

print('Sum duration of Jobs:', duration, ' seconds')


spark.stop()