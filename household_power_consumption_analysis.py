from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
import pandas as pd

spark = SparkSession.builder \
    .appName("MapReduce") \
    .getOrCreate()

df = spark.read.csv("./data/household_power_consumption.txt", sep=";", header=True)

df.show(30)

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

# Calculate maximum values for the three columns

global_active_power = "Global_active_power"
global_reactive_power = "Global_reactive_power"
voltage = "Voltage"

max_values = df.select(max("Global_active_power"), max("Global_reactive_power"), max("Voltage")).first()
max_global_active_power = max_values[0]
max_global_reactive_power = max_values[1]
max_voltage = max_values[2]

# Calculate minimum values for the three columns
min_values = df.select(min("Global_active_power"), min("Global_reactive_power"), min("Voltage")).first()
min_global_active_power = min_values[0]
min_global_reactive_power = min_values[1]
min_voltage = min_values[2]

# Calculate counts for the three columns
count1 = df.select("Global_active_power").count()
count2 = df.select("Global_reactive_power").count()
count3 = df.select("Voltage").count()

print(f"Maximum value for {global_active_power}: {max_global_active_power}")
print(f"Maximum value for {global_reactive_power}: {max_global_reactive_power}")
print(f"Maximum value for {voltage}: {max_voltage}")

print(f"Minimum value for {global_active_power}: {min_global_active_power}")
print(f"Minimum value for {global_reactive_power}: {min_global_reactive_power}")
print(f"Minimum value for {voltage}: {min_voltage}")

print(f"Count of values for {global_active_power}: {count1}")
print(f"Count of values for {global_reactive_power}: {count2}")
print(f"Count of values for {voltage}: {count3}")

spark.stop()