from pyspark.sql import SparkSession
from pyspark.sql.functions import min, mean, stddev, col
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("MapReduce") \
    .getOrCreate()

df = spark.read.csv("./data/household_power_consumption.txt", sep=";", header=True)

global_active_power = "Global_active_power"
global_reactive_power = "Global_reactive_power"
voltage = "Voltage"
global_intensity = "Global_intensity"

df.show()

df = df.filter(df.Global_active_power != "?")
df = df.filter(df.Global_reactive_power != "?")
df = df.filter(df.Voltage != "?")
df = df.filter(df.Global_intensity != "?")
# df_filtered.show()
# print("==============================================================")


df.show()
print("==============================================================")

# df.na.fill(10000).show()

# # Get the data types of all columns
# column_data_types = df.dtypes

# # Print the data types
# for column_name, data_type in column_data_types:
#     print(f"Column '{column_name}' has data type: {data_type}")


# Define the new data type you want for the column
new_data_type = DoubleType()  

# Use the `withColumns` method to change the data type of the column
df = df.withColumns({ "Global_active_power": col("Global_active_power").cast(new_data_type), \
                    "Global_reactive_power": col("Global_reactive_power").cast(new_data_type), \
                    "Voltage": col("Voltage").cast(new_data_type), \
                    global_intensity: col(global_intensity).cast(new_data_type)
                      })



# ============= Calculate maximum values for the three columns ===============
# Define lambda expression for MapReduce
map_max = lambda x: x[0]
reduce_max = lambda x, y: max(x, y)

# Peform RDD mpa and reduce to get maximum values
max_global_active_power = df.select(global_active_power).rdd.map(map_max).reduce(reduce_max)
max_global_reactive_power = df.select(global_reactive_power).rdd.map(map_max).reduce(reduce_max)
max_voltage = df.select(voltage).rdd.map(map_max).reduce(reduce_max)
max_global_intensity = df.select(global_intensity).rdd.map(map_max).reduce(reduce_max)

print(f"Maximum value for '{global_active_power}': {max_global_active_power}")
print(f"Maximum value for '{global_reactive_power}': {max_global_reactive_power}")
print(f"Maximum value for '{voltage}': {max_voltage}")
print(f"Maximum value for '{global_intensity}': {max_global_intensity}")
print("==============================================================")

# Calculate minimum values for the three columns
min_values = df.select(min(global_active_power), min(global_reactive_power), min(voltage), min(global_intensity)).first()
min_global_active_power = min_values[0]
min_global_reactive_power = min_values[1]
min_voltage = min_values[2]
min_global_intensity = min_values[3]

print(f"Minimum value for '{global_active_power}': {min_global_active_power}")
print(f"Minimum value for '{global_reactive_power}': {min_global_reactive_power}")
print(f"Minimum value for '{voltage}': {min_voltage}")
print(f"Minimum value for '{global_intensity}: {min_global_intensity}")
print("==============================================================")

# Calculate counts for the three columns
count_global_active_power = df.select(global_active_power).count()
count_global_reactive_power = df.select(global_reactive_power).count()
count_voltage = df.select(voltage).count()
count_global_intensity = df.select(global_intensity).count()

print(f"Count of values for '{global_active_power}': {count_global_active_power}")
print(f"Count of values for '{global_reactive_power}': {count_global_reactive_power}")
print(f"Count of values for '{voltage}': {count_voltage}")
print(f"Count of values for '{global_intensity}': {count_global_intensity}")
print("==============================================================")



# Calculate the mean and standard deviation 
mean_values = df.select(mean(global_active_power), mean(global_reactive_power), mean(voltage), mean(global_intensity)).first()
mean_global_active_power = mean_values[0]
mean_global_reactive_power = mean_values[1]
mean_voltage = mean_values[2]
mean_global_intensity = mean_values[3]

print(f"Mean value for '{global_active_power}': {mean_global_active_power}")
print(f"Mean value for '{global_reactive_power}': {mean_global_reactive_power}")
print(f"Mean value for '{voltage}': {mean_voltage}")
print(f"Mean value for '{global_intensity}': {mean_global_intensity}")
print("==============================================================")

# Calculate the standard deviation 
stddev_values = df.select(stddev(global_active_power), stddev(global_reactive_power), stddev(voltage), stddev(global_intensity)).first()
stddev_global_active_power = stddev_values[0]
stddev_global_reactive_power = stddev_values[1]
stddev_voltage = stddev_values[2]
stddev_global_intensity = stddev_values[3]

print(f"Standard Deviation for '{global_active_power}: {stddev_global_active_power}")
print(f"Standard Deviation for '{global_reactive_power}': {stddev_global_reactive_power}")
print(f"Standard Deviation for '{voltage}': {stddev_voltage}")
print(f"Standard Deviation for '{global_intensity}': {stddev_global_intensity}")
print("==============================================================")


# Perform min-max normalization
df_normalized = df.select(global_active_power, global_reactive_power, voltage, global_intensity)

# Define the min-max normalization function
min_max_normalize = lambda col_name, min_val, max_val: (col(col_name) - min_val) / (max_val - min_val)

# Apply the min-max normalization to the columns
df_normalized = df_normalized.withColumn("Global_active_power", min_max_normalize(global_active_power, min_global_active_power, max_global_active_power))
df_normalized = df_normalized.withColumn("Global_reactive_power", min_max_normalize(global_reactive_power, min_global_reactive_power, max_global_reactive_power))
df_normalized = df_normalized.withColumn("Voltage", min_max_normalize(voltage, min_voltage, max_voltage))
df_normalized = df_normalized.withColumn("Voltage", min_max_normalize(global_intensity, min_global_intensity, max_global_intensity))

# Rename the normalized columns 
df_normalized = df_normalized.withColumnsRenamed({global_active_power : "normalized_global_active_power", \
                                                  global_reactive_power : "normalized_global_reactive_power", \
                                                  voltage: "normalized_voltage", \
                                                  global_intensity: "normalized_global_intensity"
                                                  })

# Show the resulting DataFrame with normalized columns
df_normalized.show()
print("==============================================================")

import time

# For UI to stick
time.sleep(1000000)

spark.stop()