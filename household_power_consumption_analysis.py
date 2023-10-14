from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("MapReduce") \
    .getOrCreate()

df = spark.read.csv("./data/household_power_consumption.txt", sep=";", header=True)

df.show()

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
                    "Voltage": col("Voltage").cast(new_data_type)
                      })



# # Get the data types of all columns
# column_data_types = df.dtypes

# # Print the data types
# for column_name, data_type in column_data_types:
#     print(f"Column '{column_name}' has data type: {data_type}")



global_active_power = "Global_active_power"
global_reactive_power = "Global_reactive_power"
voltage = "Voltage"

# Calculate maximum values for the three columns
max_values = df.select(max(global_active_power), max(global_reactive_power), max(voltage)).first()
max_global_active_power = max_values[0]
max_global_reactive_power = max_values[1]
max_voltage = max_values[2]

print(f"Maximum value for '{global_active_power}': {max_global_active_power}")
print(f"Maximum value for '{global_reactive_power}': {max_global_reactive_power}")
print(f"Maximum value for '{voltage}': {max_voltage}")
print("==============================================================")

# Calculate minimum values for the three columns
min_values = df.select(min(global_active_power), min(global_reactive_power), min(voltage)).first()
min_global_active_power = min_values[0]
min_global_reactive_power = min_values[1]
min_voltage = min_values[2]

print(f"Minimum value for '{global_active_power}': {min_global_active_power}")
print(f"Minimum value for '{global_reactive_power}': {min_global_reactive_power}")
print(f"Minimum value for '{voltage}': {min_voltage}")
print("==============================================================")

# Calculate counts for the three columns
count_global_active_power = df.select(global_active_power).count()
count_global_reactive_power = df.select(global_reactive_power).count()
count_voltage = df.select(voltage).count()

print(f"Count of values for '{global_active_power}': {count_global_active_power}")
print(f"Count of values for '{global_reactive_power}': {count_global_reactive_power}")
print(f"Count of values for '{voltage}': {count_voltage}")
print("==============================================================")



# Calculate the mean and standard deviation 
mean_values = df.select(mean(global_active_power), mean(global_reactive_power), mean(voltage)).first()
mean_global_active_power = mean_values[0]
mean_global_reactive_power = mean_values[1]
mean_voltage = mean_values[2]

print(f"Mean value for '{global_active_power}': {mean_global_active_power}")
print(f"Mean value for '{global_reactive_power}': {mean_global_reactive_power}")
print(f"Mean value for '{voltage}': {mean_voltage}")
print("==============================================================")

# Calculate the standard deviation 
stddev_values = df.select(stddev(global_active_power), stddev(global_reactive_power), stddev(voltage)).first()
stddev_global_active_power = stddev_values[0]
stddev_global_reactive_power = stddev_values[1]
stddev_voltage = stddev_values[2]

print(f"Standard Deviation for '{global_active_power}: {stddev_global_active_power}")
print(f"Standard Deviation for '{global_reactive_power}': {stddev_global_reactive_power}")
print(f"Standard Deviation for '{voltage}': {stddev_voltage}")
print("==============================================================")


spark.stop()