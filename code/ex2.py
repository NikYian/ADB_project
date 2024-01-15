from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('ex2') \
        .config("spark.master", "yarn") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
        
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Read the CSVs file into a DataFrames
df1 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
df2 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)

df = df1.union(df2)

df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("DATE OCC", to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.select("Date Rptd", "DATE OCC", "Vict Age", "LAT", "LON")

print("Dataframe Schema:")
df.printSchema()

print("Number of rows in the DataFrame:")

print(df.count())
