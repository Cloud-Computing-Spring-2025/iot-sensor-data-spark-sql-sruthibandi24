from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("IoT Sensor Task 2").getOrCreate()

# ✅ Correct input path to match data generator output
df = spark.read.option("header", True).option("inferSchema", True).csv("input/sensor_data.csv")

# Filter readings
in_range = df.filter((df.temperature >= 18) & (df.temperature <= 30))
out_of_range = df.filter((df.temperature < 18) | (df.temperature > 30))

# Print counts
print("In-range count:", in_range.count())
print("Out-of-range count:", out_of_range.count())

# Aggregation by location
agg = df.groupBy("location").avg("temperature", "humidity") \
    .withColumnRenamed("avg(temperature)", "avg_temperature") \
    .withColumnRenamed("avg(humidity)", "avg_humidity") \
    .orderBy("avg_temperature", ascending=False)

# ✅ Save result
agg.write.mode("overwrite").csv("task2_output.csv", header=True)
