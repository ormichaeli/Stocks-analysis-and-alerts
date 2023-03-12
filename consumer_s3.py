from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType
import json


with open('/tmp/pycharm_project_296/config.json') as f:
    config = json.load(f)
    access_key = config['Access_key_ID']
    secret_key = config['Secret_access_key']

endpoint="s3.amazonaws.com"

conf = SparkConf()
conf.set("fs.s3a.access.key", access_key)
conf.set("fs.s3a.secret.key", secret_key)
conf.set("fs.s3a.endpoint", endpoint)

bootstrapServers = "Cnt7-naya-cdh63:9092"
# for reading from kafka
topic3 = "stocks_prices_to_s3"

spark = SparkSession \
    .builder \
    .appName("real_time_prices_s3") \
    .config(conf=conf) \
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

# cast to currect types
df_realtime_prices = df_realtime_prices.select(col("stock_ticker"),col("current_price").cast("float"), col("time").cast("timestamp"))

stream_to_s3 = df_realtime_prices \
    .writeStream \
    .format("json") \
    .partitionBy('stock_ticker') \
    .option("path", "s3a://realtime-stocks-prices-deproj/") \
    .option("checkpointLocation", "s3a://checkpoint-s3/") \
    .start() \
    .awaitTermination()