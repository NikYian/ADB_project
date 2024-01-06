import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year,udf, mean,format_number, count,row_number
from pyspark.sql.types import  FloatType
from geopy.distance import geodesic
from pyspark.sql.window import Window


os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
spark = SparkSession.builder.appName('Q4') \
    .config("spark.archives",  # 'spark.yarn.dist.archives' in YARN.
    "pyspark_conda_env.tar.gz#environment")\
        .config("spark.master", "yarn") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g").getOrCreate()
        
# Read the CSVs file into a DataFrames
df1 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
df2 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)

df = df1.union(df2)
df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Year", year("Date Rptd")).drop("Date Rptd")

df = df.select("DR_NO","Year","AREA ","Weapon Used Cd", "LAT", "LON")

# filter fire arm crimes
df = df.filter(df["Weapon Used Cd"].cast("int").between(100, 199))

# remove Null Island entries
df = df.filter((df["LAT"] != 0) & (df["LON"] != 0))

police_stations = spark.read.csv('hdfs://okeanos-master:54310/user/project/LAPD_Police_Stations.csv', header=True, inferSchema=True)

police_stations = police_stations.select("DIVISION","X", "Y","PREC")

joined_df = df.join(police_stations, (df["AREA "] == police_stations.PREC)).drop("AREA ", "Weapon Used Cd","PREC")

def get_distance(lat1, lon1, lat2, lon2):
    distance_km = geodesic((lat1, lon1), (lat2, lon2)).km
    rounded_distance_km = round(distance_km, 3)
    return rounded_distance_km

get_distance = udf(get_distance, FloatType())

distance_df = joined_df.withColumn("Distance", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))

result_a = distance_df.groupBy("Year").agg(
    mean("Distance").alias("Mean_Distance (km)"),
    count("*").alias("#")
).orderBy(col("Year"))

result_a = result_a.withColumn("Mean_Distance (km)", format_number("Mean_Distance (km)", 3))

result_b = distance_df.groupBy("DIVISION").agg(
    mean("Distance").alias("Mean_Distance (km)"),
    count("*").alias("#")
).orderBy(col("#").desc())

result_b = result_b.withColumn("Mean_Distance (km)", format_number("Mean_Distance (km)", 3))

print('Q4.1')

result_a.show()

result_b.show(result_b.count())

joined_df = df.crossJoin(police_stations) \
    .withColumn("Distance", get_distance(col("LAT"), col("LON"), col("Y"), col("X"))) 
    
window_spec = Window.partitionBy("DR_NO").orderBy("Distance")
result_df = joined_df.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 1)

# Drop the additional column used for window function
result_df = result_df.drop("row_number")

result_a = result_df.groupBy("Year").agg(
    mean("Distance").alias("Mean Distance From Closest (km)"),
    count("*").alias("#")
).orderBy(col("Year"))

result_a = result_a.withColumn("Mean Distance From Closest (km)", format_number("Mean Distance From Closest (km)", 3))

result_b = result_df.groupBy("DIVISION").agg(
    mean("Distance").alias("Mean Distance From Closest (km)"),
    count("*").alias("#")
).orderBy(col("#").desc())

result_b = result_b.withColumn("Mean Distance From Closest (km)", format_number("Mean Distance From Closest (km)", 3))

print('Q4.2')

result_a.show()

result_b.show(result_b.count())

spark.stop()