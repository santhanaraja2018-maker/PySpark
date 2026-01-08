from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApplication").getOrCreate()


#creating an RDD from a list of tuples
data = [("Alice",25),("bob",50),("Charlie",35),("Alice",45)]
rdd = spark.sparkContext.parallelize(data)

Count = rdd.count()
print("Total number of elments in RDD : ",Count)

first = rdd.first()
print("First Element : ",first)

take_elements = rdd.take(2)
print ("The elements taken : ",take_elements)


#for each action
rdd.foreach(lambda x: print(x))


spark.stop()