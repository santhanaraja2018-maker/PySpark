from pyspark import SparkContext

#create a SparkContext object

sc = SparkContext(appName="MySparkApplication")

print(sc)


# to shutdown the current active spark context

sc.stop() # stops spark context
