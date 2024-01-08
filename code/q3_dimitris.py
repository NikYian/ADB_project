from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace
from pyspark.sql.functions import year
#import numpy as np

# Create a Spark session with specified configurations
spark = SparkSession.builder \
        .appName('Q2_DF') \
        .config("spark.master", "yarn") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

  # Read the CSVs file into a DataFrames
df1 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
df2 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)

  # Union the DataFrames
df = df1.union(df2)

  # Select relevant columns and filter out rows with null values in 'Vict Descent'
df = df.select('Date Rptd', 'Vict Descent','LAT', 'LON').filter(col('Vict Descent').isNotNull())

  # Convert 'Date Rptd' to date format and extract the 'Year'
df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Year", year("Date Rptd")).drop("Date Rptd").filter(col("Year") == 2015).drop('Year')

  # Read income data and format the 'Estimated Median Income' column
income = spark.read.csv('hdfs://okeanos-master:54310/user/project/income/LA_income_2015.csv', header=True, inferSchema=True)
income = income.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", "[^0-9]", "").cast("int"))

  # Read geocoding data and rename columns for clarity
geocoding = spark.read.csv('hdfs://okeanos-master:54310/user/project/revgecoding.csv', header=True, inferSchema=True)
geocoding = geocoding \
      .withColumnRenamed("LAT", "LAT_g") \
      .withColumnRenamed("LON", "LON_g") 
  

df.write.parquet("hdfs://okeanos-master:54310/user/project/zitima_7_data", mode='overwrite')
# Join crime data with geocoding data based on matching LAT and LON values
df = df.join(geocoding, (df.LAT == geocoding.LAT_g) & (df.LON == geocoding.LON_g)).drop("LAT","LON","LAT_g","LON_g")

  # Select distinct ZIPcodes from geocoding data
distinct_geocoding = geocoding.select("ZIPcode").distinct()

  # Filter income data based on distinct ZIPcodes
filtered_income = income.join(distinct_geocoding, income["Zip Code"] == distinct_geocoding["ZIPcode"])

  # Order income data by 'Estimated Median Income' in descending order
filtered_income = filtered_income.orderBy(col("Estimated Median Income").desc())

  # Get top 3 zip codes with highest Estimated Median Income
top3 = filtered_income.limit(3)

  # Get bottom 3 zip codes with lowest Estimated Median Income
filtered_income = filtered_income.orderBy(col("Estimated Median Income"))
tail3 = filtered_income.limit(3)

join_top3 = df.join(top3, (df.ZIPcode == top3.ZIPcode))
count_top3 = join_top3.groupBy("Vict Descent").count().orderBy(col("count").desc())

  # Join crime data with the top 3 ZIPcodes and count occurrences by 'Vict Descent'
join_tail3 = df.join(tail3, (df.ZIPcode == top3.ZIPcode))
count_tail3 = join_tail3.groupBy("Vict Descent").count().orderBy(col("count").desc())

  # Mapping for victim descent codes
descent_mapping = {
      "A": "Other Asian",
      "B": "Black",
      "C": "Chinese",
      "D": "Cambodian",
      "F": "Filipino",
      "G": "Guamanian",
      "H": "Hispanic/Latin/Mexican",
      "I": "American Indian/Alaskan Native",
      "J": "Japanese",
      "K": "Korean",
      "L": "Laotian",
      "O": "Other",
      "P": "Pacific Islander",
      "S": "Samoan",
      "U": "Hawaiian",
      "V": "Vietnamese",
      "W": "White",
      "X": "Unknown",
      "Z": "Asian Indian"
}

  # Replace victim descent codes 
count_top3 = count_top3.withColumn("Victim Descent", col("Vict Descent").cast("string")).replace(descent_mapping, subset=["Victim Descent"]).drop("Vict Descent")
columns_order = ["Victim Descent", "count"] 
count_top3 = count_top3.select(columns_order)
count_tail3 = count_tail3.withColumn("Victim Descent", col("Vict Descent").cast("string")).replace(descent_mapping, subset=["Victim Descent"]).drop("Vict Descent")
count_tail3 = count_tail3.select(columns_order)

count_tail3.show()
print('TAIL 3')