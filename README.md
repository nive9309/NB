from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("EarthquakeAnalysis") \
    .getOrCreate()

# Assuming the dataset is in a CSV file
dataset_path = "/Users/Desktop/database 3.csv"


df = spark.read.csv(dataset_path, header=True, inferSchema=True)

from pyspark.sql.functions import concat, col, to_timestamp


df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), col("Time")), "yyyy-MM-ddHH:mm:ss"))
df = df.drop("Date", "Time")
df = df.filter(col("Magnitude") > 5.0)
from pyspark.sql.functions import avg


df_avg = df.groupBy("Type").agg(avg("Depth").alias("AverageDepth"), avg("Magnitude").alias("AverageMagnitude"))
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType



def categorize_magnitude(magnitude):
    if magnitude < 5.0:
        return "Low"
    elif magnitude < 7.0:
        return "Moderate"
    else:
        return "High"

categorize_magnitude_udf = udf(categorize_magnitude, StringType())


df = df.withColumn("MagnitudeLevel", categorize_magnitude_udf(col("Magnitude")))
from pyspark.sql.functions import lit, sqrt



reference_latitude = 0.0
reference_longitude = 0.0

df = df.withColumn("DistanceFromReference", sqrt((col("Latitude") - lit(reference_latitude))**2 + (col("Longitude") - lit(reference_longitude))**2))


pip install folium

import folium

# Create a map centered at the reference location
map_center = [reference_latitude, reference_longitude]
m = folium.Map(location=map_center, zoom_start=2)

# Add markers for each earthquake
for row in df.collect():
    latitude = row["Latitude"]
    longitude = row["Longitude"]
    magnitude = row["Magnitude"]
    popup = f"Magnitude: {magnitude}"
    folium.Marker([latitude, longitude], popup=popup).add_to(m)

# Save the map as an HTML file
m.save("earthquake_map.html")

output_path = "Users/nivehithabalaji/Desktop/database 4.csv"
df.write.csv(output_path, header=True)

