import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
df = spark.createDataFrame(data)
df.show()

time.sleep(120)
