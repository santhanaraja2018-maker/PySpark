from pyspark.sql import SparkSession

#create spark session

spark = SparkSession.builder.appName("MySparkApplication").getOrCreate()

print(spark)

sc = spark.sparkContext

print(sc)

# to shutdown the current active spark context

sc.stop() # stops spark context

spark.stop() # stops spark session