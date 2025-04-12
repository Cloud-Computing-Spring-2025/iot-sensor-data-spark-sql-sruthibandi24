from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, to_timestamp

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 5").getOrCreate()

# ✅ Load CSV with corrected path
df = spark.read.option("header", True).option("inferSchema", True).csv("input/sensor_data.csv")

# Convert timestamp string to proper timestamp type
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Extract hour of the day from timestamp
df = df.withColumn("hour_of_day", hour("timestamp"))

# Pivot table: location as rows, hour_of_day as columns, avg temperature as values
pivot_df = df.groupBy("location").pivot("hour_of_day").agg(avg("temperature"))

# ✅ Save pivoted data to CSV
pivot_df.write.mode("overwrite").csv("task5_output.csv", header=True)

# (Optional) Find and print the hottest (location, hour_of_day)
flat_df = df.groupBy("location", "hour_of_day").agg(avg("temperature").alias("avg_temp"))
hottest = flat_df.orderBy(flat_df.avg_temp.desc()).limit(1).collect()[0]

print(f"🔥 Hottest spot: Location = {hottest['location']}, Hour = {hottest['hour_of_day']}, Avg Temp = {hottest['avg_temp']:.2f}")
