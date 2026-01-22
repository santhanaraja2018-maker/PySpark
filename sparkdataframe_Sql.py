from pyspark.sql import SparkSession

#create session
spark = SparkSession.builder.appName("dataframesql").master("local[*]").getOrCreate()

#loading data
data_file_path = r"C:\Users\SANTHANA RAJA S\Desktop\Git\PySpark\customers.csv"
df= spark.read.csv(data_file_path, header=True, inferSchema=True)

#display schema
df.printSchema()

#show initial data frame
df.show(10)


#registering data frame as temproary table to perform sql operations

df.createOrReplaceTempView("my_table")


#print age > 25
result = spark.sql("select * from my_table where age > 35")
print("\ndatas with age > 35")
result.show()

#average salary of male and female

avg_sal = spark.sql ( " select Gender,avg(Amount) as AVG_SALARY  from my_table group by Gender ")
print("\nAverage male and female salary")
avg_sal.show()


spark.stop()