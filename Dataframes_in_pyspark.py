
from pyspark.sql import SparkSession

#create spark session
spark = SparkSession.builder.appName("Create-Data-Frame-in-Pyspark").master("local[*]").getOrCreate()

#read csv
df = spark.read.csv(r"C:\Users\SANTHANA RAJA S\Desktop\Git\PySpark\data.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

#write json file
# df.write.json(r"C:\Users\SANTHANA RAJA S\Desktop\Git\PySpark\data.json")

#read json
# df = spark.read.json(r"C:\Users\SANTHANA RAJA S\Desktop\Git\PySpark\data.json", multiline=True, inferSchema=True)
# df.printSchema()
# df.show(8)


#write parquet file
df.write.parquet(r"C:\Users\SANTHANA RAJA S\Desktop\Git\PySpark\data.parquet")

#read parquet file
df = spark.read.parquet(r"C:\Users\SANTHANA RAJA S\Desktop\Git\PySpark\data.parquet")
df.printSchema()
df.show(3)

spark.stop()