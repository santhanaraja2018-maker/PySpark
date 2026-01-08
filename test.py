import findspark
findspark.init()  # Sets Spark environment paths

from pyspark.sql import SparkSession
# Create Spark session
spark = SparkSession.builder.appName("VSCodeTest").getOrCreate()

data = [("Alice", 10), ("Bob", 20), ("Cathy", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()

df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people WHERE Age > 15").show()

rdd = spark.sparkContext.parallelize([1,2,3,4,5])
op=rdd.map(lambda x: x*2).collect()
print(op)