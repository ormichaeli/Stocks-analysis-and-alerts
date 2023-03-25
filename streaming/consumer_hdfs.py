from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType

bootstrapServers = "Cnt7-naya-cdh63:9092"
# for reading from kafka
topic3 = "stocks_prices_to_hdfs"

spark = SparkSession \
    .builder \
    .appName("realtime_prices_hdfs") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3') \
    .getOrCreate()

# ReadStream from kafka
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", topic3)\
    .load()


#Create schema to create df from json
schema = StructType() \
    .add("stock_ticker", StringType()) \
    .add("current_price", StringType()) \
    .add("time", StringType())

df_realtime_prices = df_kafka.select(col("value").cast("string"))\
    .select(from_json(col("value"), schema).alias("value"))\
    .select("value.*")

# cast to correct types
df_realtime_prices = df_realtime_prices.select(col("stock_ticker"),col("current_price").cast("float"), col("time").cast("timestamp"))
# adding a column that contains only the date, for the partition in hdfs
df_realtime_prices = df_realtime_prices \
    .withColumn("date", date_format("time", "yyyy-MM-dd"))

stream_to_hdfs = df_realtime_prices \
    .writeStream \
    .format("json") \
    .partitionBy('date', 'stock_ticker') \
    .option("path", "/user/naya/realtime_prices1/") \
    .option("checkpointLocation", "/user/naya/realtime_prices_checkPoint1") \
    .start() \
    .awaitTermination()