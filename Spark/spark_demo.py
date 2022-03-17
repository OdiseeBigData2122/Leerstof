from pyspark.sql import SparkSession

# eerst een spark sessie aanmaken
spark = SparkSession.builder.config("spark.driver.cores", 2).appName("oefening_spark_les").getOrCreate()

# lees csv
df = spark.read.option("delimiter", ",").option("header", True).csv("/user/bigdata/06_Spark/demo/input.csv")
df.show()
