from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IoT Task 1").getOrCreate()

# ✅ Corrected path to match your data generator output
df = spark.read.option("header", True).option("inferSchema", True).csv("input/sensor_data.csv")

df.createOrReplaceTempView("sensor_readings")

# Show first 5 rows
df.show(5)

# Count records
print("Total records:", df.count())

# Distinct locations
spark.sql("SELECT DISTINCT location FROM sensor_readings").show()

# Distinct sensor types
spark.sql("SELECT DISTINCT sensor_type FROM sensor_readings").show()

# Save output
df.write.csv("task1_output.csv", header=True)
