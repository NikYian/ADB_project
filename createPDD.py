from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Project').getOrCreate()

from pyspark.sql.functions import col, to_date

columns_to_select = ["Date Rptd", "DATE OCC", "Vict Age","LAT","LON"]

# Read the CSV file into a DataFrame
df1 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True).select(columns_to_select)

df1 = df1.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a'))
df1 = df1.withColumn("DATE OCC", to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a'))

# Read the CSV file into a DataFrame
df2 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True).select(columns_to_select)

df2 = df2.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a'))
df2 = df2.withColumn("DATE OCC", to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a'))

df = df1.union(df2)

df.show()