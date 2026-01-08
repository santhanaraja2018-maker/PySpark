from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApplication").getOrCreate()

numbers = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(numbers)

#collect action retrive all elements of the RDD
print(rdd.collect())

#creating an RDD from a list of tuples
data = [("Alice",25),("bob",50),("Charlie",35),("Alice",45)]
rdd = spark.sparkContext.parallelize(data)

#collect action retrive all elements of the RDD
print("All elements of the rdd : ",rdd.collect())

spark.stop()
