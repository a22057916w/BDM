from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from math import sqrt


# spark = SparkSession.builder \
#     .appName("MapReduce") \
#     .config("spark.network.timeout", "100s") \
#     .getOrCreate()
# sc = spark.sparkContext
conf = SparkConf().setAppName("MapReduce")
sc = SparkContext(conf=conf)




from operator import add

rdd = sc.parallelize(["a", "b", "c"])
result = rdd.map(lambda x: (x, 1))

print(result.collect())


# df = spark.read.csv("./data/household_power_consumption.txt", sep=";", header=True)
# df = df.select(df.Global_active_power, df.Global_reactive_power, df.Voltage)
# df = df.limit(2)
# df.show()
# # Load the input text file as an RDD
# input_rdd = df.rdd.map(lambda x: x)
# input_rdd.toDF().show()
# print(input_rdd.collect())
# print(input_rdd.collect()[0])
# print(input_rdd.collect()[0][0])
# # Display the contents of the input_rdd
# for line in input_rdd.collect():
#     print(line)


# Calculate statistics using RDD transformations for each column
columns = ["Global_active_power", "Global_reactive_power", "Voltage"]

# Stop the SparkContext
# spark.stop()