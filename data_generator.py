import csv
import random
import os
from faker import Faker

# Initialize Faker instance
fake = Faker()

# Constants
LOCATIONS = ["BuildingA_Floor1", "BuildingA_Floor2", "BuildingB_Floor1", "BuildingB_Floor2"]
SENSOR_TYPES = ["TypeA", "TypeB", "TypeC"]

def generate_sensor_data(num_records=1000, output_file="input/sensor_data.csv"):
    """
    Generates a CSV file with fields:
    sensor_id, timestamp, temperature, humidity, location, sensor_type
    """
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    fieldnames = ["sensor_id", "timestamp", "temperature", "humidity", "location", "sensor_type"]

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_records):
            sensor_id = random.randint(1000, 1100)  # Sensor ID between 1000–1100
            timestamp = fake.date_time_between(start_date="-5d", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
            temperature = round(random.uniform(15.0, 35.0), 2)  # Temperature in °C
            humidity = round(random.uniform(30.0, 80.0), 2)     # Humidity in %
            location = random.choice(LOCATIONS)
            sensor_type = random.choice(SENSOR_TYPES)

            writer.writerow({
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "location": location,
                "sensor_type": sensor_type
            })

if __name__ == "__main__":
    generate_sensor_data()
    print("✅ sensor_data.csv successfully generated in the 'input/' folder.")
