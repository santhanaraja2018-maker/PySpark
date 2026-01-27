
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("MyApplication").getOrCreate())


#creating an RDD from a list of tuples
data = [("Alice",25),("Bob",50),("Charlie",35),("Alice",45)]
rdd = spark.sparkContext.parallelize(data)
print("\nRdd elements  \n",rdd)

#Map transformation : convert name to upper
mapped_rdd = rdd.map(lambda x : (x[0].upper(),x[1]))
result = mapped_rdd.collect()
print("\nRdd elements after capitilizing names \n",result)

#filter transformation : filter records with where condition
filtered_rdd_age = rdd.filter(lambda x : x[1]>30)
result = filtered_rdd_age.collect()
print("\nRdd elments with age > 30 : ",result)

filtered_rdd_name= rdd.filter(lambda x : "bob" in x[0].lower())   #%like% in sql
result = filtered_rdd_name.collect()
print("\nRdd elments with name like bob : ",result)

#Reduce key transformation : calculate the total age for each name
reduced_rdd = rdd.reduceByKey(lambda x, y : x+y)
result = reduced_rdd.collect()
print("\nRdd elments with age added for each name : ",result)

#sort by age in descending order
sorted_rdd = rdd.sortBy(lambda x : x[1], ascending = False)
result = sorted_rdd.collect()
print("\nRdd elments sort by age in descending order : ",result)

# to save rdds as text file
rdd.saveAsTextFile("output")

#create or read rdd from text file, read is not working in our lap
rdd_text = spark.sparkContext.textFile("output")
result = rdd_text.collect()
print("\nRdd elments from text file : ",result)

spark.stop()