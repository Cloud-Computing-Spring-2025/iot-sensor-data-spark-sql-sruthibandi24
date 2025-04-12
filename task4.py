from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, dense_rank
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 4").getOrCreate()

# ✅ Corrected input path
df = spark.read.option("header", True).option("inferSchema", True).csv("input/sensor_data.csv")

# Average temperature per sensor
avg_df = df.groupBy("sensor_id").agg(avg("temperature").alias("avg_temp"))

# Rank sensors by average temperature using a window function
windowSpec = Window.orderBy(avg_df["avg_temp"].desc())
ranked = avg_df.withColumn("rank_temp", dense_rank().over(windowSpec))

# Save top 5 sensors to CSV
ranked.limit(5).write.mode("overwrite").csv("task4_output.csv", header=True)
