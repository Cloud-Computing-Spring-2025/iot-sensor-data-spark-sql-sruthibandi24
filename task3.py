from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, to_timestamp

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 3").getOrCreate()

# ✅ Correct CSV path
df = spark.read.option("header", True).option("inferSchema", True).csv("input/sensor_data.csv")

# Convert timestamp column to proper timestamp format
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Register as temp view (optional but useful for SQL-style queries)
df.createOrReplaceTempView("sensor_readings")

# Extract hour from timestamp
df = df.withColumn("hour_of_day", hour("timestamp"))

# Group by hour and calculate average temperature
result = df.groupBy("hour_of_day").agg(avg("temperature").alias("avg_temp")).orderBy("hour_of_day")

# ✅ Save result to CSV
result.write.mode("overwrite").csv("task3_output.csv", header=True)
