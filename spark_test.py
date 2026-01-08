import findspark
findspark.init()  # Sets Spark environment paths

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("VSCodeTest").getOrCreate()

# Create a DataFrame
df = spark.range(5)
df.show()

# Stop Spark
spark.stop().