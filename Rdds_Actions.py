from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApplication").getOrCreate()


#creating an RDD from a list of tuples
data = [("Alice",25),("Bob",50),("Charlie",35),("Alice",45)]
rdd = spark.sparkContext.parallelize(data)
print("\n Rdd elements : \n",rdd)

Count = rdd.count()
print("\nTotal number of elments in RDD : ",Count)

first = rdd.first()
print("\nFirst Element : ",first)

take_elements = rdd.take(2)
print ("\nThe elements taken : ",take_elements)


#for each action to print elements of rdd
rdd.foreach(lambda x: print(x))


spark.stop()