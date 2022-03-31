
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.master("local").appName("Structured Streaming les").getOrCreate()

# extract
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# transform
words = lines.select(explode(split(lines.value, " ")).alias("word"))
wordCounts = words.groupBy("word").count()

# load #### dit is aangepast
app = wordCounts.writeStream.outputMode("update").format("console").start()
app.awaitTermination()
