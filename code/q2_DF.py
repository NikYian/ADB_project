from app_duration import AppDuration
import numpy as np

def Q2_DF():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, unix_timestamp, from_unixtime, date_format
    # Create a Spark session with specified configurations
    spark = SparkSession.builder \
            .appName('Q2_DF') \
            .config("spark.master", "yarn") \
            .config("spark.executor.instances", "4") \
            .config("spark.executor.cores", "1") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()

    # Read the CSVs file into a DataFrames
    df1 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=False).select("TIME OCC","Premis Cd")
    df2 = spark.read.csv('hdfs://okeanos-master:54310/user/project/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=False).select("TIME OCC","Premis Cd")

    df = df1.union(df2)


    # Convert the 'TIME OCC' column to a timestamp
    df = df.withColumn(
        "TIME OCC",
        from_unixtime(unix_timestamp(col("TIME OCC"), "HHmm")).cast("timestamp")
    )

    df = df.withColumn(
        "TIME OCC",
        date_format(col("TIME OCC").cast("timestamp"), "HH:mm:ss")
    )

    df = df.withColumn(
        "Premis Cd",
        col("Premis Cd").cast("int"))

    from pyspark.sql.functions import col, when, sum

    filtered_df = df.filter(col("Premis Cd") == 101).select("TIME OCC")

    # Define time intervals
    morning_interval = ((col("TIME OCC") >= "05:00:00") & (col("TIME OCC") < "12:00:00"))
    afternoon_interval = ((col("TIME OCC") >= "12:00:00") & (col("TIME OCC") < "17:00:00"))
    evening_interval = ((col("TIME OCC") >= "17:00:00") & (col("TIME OCC") < "21:00:00"))
    night_interval = ((col("TIME OCC") >= "21:00:00") | (col("TIME OCC") < "05:00:00"))

    # Apply conditions and sum within each interval
    result_df = filtered_df.groupBy().agg(
        sum(when(morning_interval, 1).otherwise(0)).alias("Morning"),
        sum(when(afternoon_interval, 1).otherwise(0)).alias("Afternoon"),
        sum(when(evening_interval, 1).otherwise(0)).alias("Evening"),
        sum(when(night_interval, 1).otherwise(0)).alias("Night")
    )

    # Show the result
    result_df.show(truncate=False)

    # # Save the DataFrame to a CSV file
    # result_df \
    #   .coalesce(1) \
    #   .write \
    #   .mode('overwrite') \
    #   .option('header', 'true') \
    #   .csv('results/q2_DF.csv')

    # import subprocess

    # hdfs_path = "hdfs://okeanos-master:54310/user/user/results/q2_DF.csv"
    # local_path = "/home/user/Project/results/"

    # subprocess.run(["hadoop", "fs", "-copyToLocal", "-f", hdfs_path, local_path])

    # Stop the Spark session
    app_id = spark.sparkContext.applicationId


    duration = AppDuration(app_id)

    # print('Sum duration of Jobs:', duration, ' seconds')


    spark.stop()
    
    return duration

if __name__ == "__main__":
  
    durations = []

    for i in range(5):
        
        duration = Q2_DF()

        durations.append(duration)

    mean_duration = np.mean(durations)
    std_duration = np.std(durations)
    result_stats = {"mean": mean_duration, "std": std_duration}
        
    # Print mean and std results
    print(f"Mean Duration: {result_stats['mean']:.2f} seconds, Std: {result_stats['std']:.2f}")