# IoT Sensor Data Analysis with Spark SQL

This project analyzes IoT sensor data using Apache Spark and Spark SQL. It demonstrates loading and transforming structured data, applying SQL queries, aggregations, time-based analysis, window functions, and pivot tables. The dataset contains IoT sensor readings including temperature, humidity, timestamps, and locations.

---

## 📁 Dataset

- **Filename**: `sensor_data.csv`
- **Columns**:
  - `sensor_id`
  - `timestamp`
  - `temperature`
  - `humidity`
  - `location`
  - `sensor_type`

---

## ✅ Task 1: Load & Basic Exploration

**Steps**:
- Load `sensor_data.csv` into a Spark DataFrame.
- Infer schema and create a temporary SQL view `sensor_readings`.
- Perform basic queries: show first 5 rows, count records, and list distinct locations.

**Sample Output:**

```text
+---------+-------------------+-----------+--------+-------------------+-----------+
|sensor_id|timestamp          |temperature|humidity|location           |sensor_type|
+---------+-------------------+-----------+--------+-------------------+-----------+
|1007     |2025-03-28 08:15:22|34.5       |72.3    |BuildingA_Floor1   |TypeA      |
|1010     |2025-03-29 11:42:09|29.1       |65.0    |BuildingB_Floor2   |TypeC      |
|1003     |2025-03-29 16:10:47|15.9       |55.4    |BuildingA_Floor2   |TypeB      |
|1007     |2025-03-30 03:25:01|20.3       |63.2    |BuildingA_Floor1   |TypeA      |
|1040     |2025-03-30 09:20:10|28.7       |61.1    |BuildingB_Floor1   |TypeA      |
+---------+-------------------+-----------+--------+-------------------+-----------+
```
## ✅ Task 2: Filtering & Simple Aggregations


**Steps**:
- Filter out temperature readings outside 18–30°C.
- Count in-range vs. out-of-range records.
- Group by location and calculate average temperature and humidity.

**Sample Output:**
```text
+-------------------+-----------------+------------------+
|location           |avg_temperature  |avg_humidity      |
+-------------------+-----------------+------------------+
|BuildingA_Floor1   |25.9             |63.4              |
|BuildingA_Floor2   |27.2             |66.1              |
|BuildingB_Floor1   |26.7             |60.8              |
|BuildingB_Floor2   |28.3             |65.0              |
+-------------------+-----------------+------------------+
```
## ✅ Task 3: Time-Based Analysis


**Steps**:
- Convert timestamp column to timestamp type.
- Extract hour_of_day from timestamps.
- Group by hour and calculate average temperature.

**Sample Output:**

```text
+-----------+-----------+
|hour_of_day|avg_temp   |
+-----------+-----------+
|0          |25.4       |
|1          |26.8       |
|2          |24.9       |
|...        |...        |
|23         |28.2       |
+-----------+-----------+
```
## ✅ Task 4: Window Function – Sensor Ranking


**Steps**:
- Calculate average temperature per sensor.
- Rank sensors using RANK() or DENSE_RANK() in descending order.

**Sample Output:**

```text
+---------+-----------+---------+
|sensor_id|avg_temp   |rank_temp|
+---------+-----------+---------+
|1010     |30.2       |1        |
|1040     |29.7       |2        |
|1007     |29.1       |3        |
|1053     |28.9       |4        |
|1003     |28.7       |5        |
+---------+-----------+---------+
```
## ✅ Task 5: Pivot & Interpretation


**Steps**:
- Create a pivot table using location as rows and hour_of_day as columns.
- Populate cells with average temperature.
- Determine the (location, hour) pair with the highest temperature.
**Sample Output:**

```text
location           | 0    | 1    | 2    | ... | 23
---------------------------------------------------
BuildingA_Floor1   |25.6  |24.9  |23.7  | ... |26.3
BuildingA_Floor2   |27.2  |25.0  |24.8  | ... |28.0
BuildingB_Floor1   |26.8  |25.4  |25.9  | ... |27.2
BuildingB_Floor2   |28.1  |26.2  |26.0  | ... |28.5
```
## Technologies Used

- Apache Spark
- PySpark / Spark SQL
- CSV file handling
- DataFrame transformations
- Window functions
- Pivot tables

---

## How to Run

1. Ensure you have Apache Spark installed and configured.
2. Place `sensor_data.csv` in the project directory.
3. Run each task's script individually (e.g., `task1.py`, `task2.py`, ...).
4. Check the corresponding output CSVs generated for each task.

---





