from pyspark.sql import SparkSession

#creating session
spark = SparkSession.builder.appName("dataframe-operations").master("local[*]").getOrCreate()


#load data into a data frame
data_file_path = r"C:\Users\SANTHANA RAJA S\Desktop\Git\PySpark\data.csv"
df = spark.read.csv(data_file_path, header=True, inferSchema=True)

df.printSchema()

#showing initial datas
print ("\nInitial datas sample \n")
df.show(10)


#select specific columns
selected_columns = df.select("Sales Person","Country","Date")
print("\nSelected Columns\n")
selected_columns.show(10)


#filter
from pyspark.sql.functions import col
filtered_data = df.filter(
    (col("Boxes Shipped") > 200) & (df.Country =="India")) #if column name has space use co1() else df.columnname
print("\nFilterd Data : ",filtered_data.count())
filtered_data.show()


#grouping and aggregation

grouped_data = df.groupBy("Country").agg({"Boxes Shipped": "sum"})
print("\nGrouped data")
grouped_data.show()

#join with another dataframe

df2=df.select("Sales Person","Product").limit(10)
joined_data = df.join(df2,"Sales Person","inner")
print("\nJoined data")
joined_data.show()

#sorting data

sorted_data = df.orderBy("Boxes Shipped")  #it does ascending
print("\nSorted data")
sorted_data.show(10)

sorted_data = df.orderBy(col("Boxes Shipped").desc())
print("\nSorted data descending")
sorted_data.show(10)

#or 

sorted_data = df.orderBy("Boxes Shipped",ascending=False)
print("\nSorted data descending")
sorted_data.show(10)

#Distinct 

distinct_rows = df.select("Country").distinct()
print("\nDistinct country")
distinct_rows.show()


#Drop columns

dropped_columns_data = df.drop("Date","Amount")
print("\nDropped column data")
dropped_columns_data.show(10)


#Add new calculated columns

df_with_new_column = df.withColumn("new boxes shipped", col("Boxes Shipped") + 100)
print("\nData frame with added column")
df_with_new_column.show(10)


#Alias renaming column
df_with_alias = df.withColumnRenamed("Amount", "Product_Price")
print("Data frame with aliased column")
df_with_alias.show(10)



spark.stop()


