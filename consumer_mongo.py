from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType

bootstrapServers = "Cnt7-naya-cdh63:9092"
# for reading from kafka
topic2 = "stocks_prices_to_mongo"

spark = SparkSession \
    .builder \
    .appName("realtime_prices_mongo") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/stocks_db.users") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/stocks_db.realtime_data") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3') \
    .getOrCreate()

## config 1: defining the default collection that we read from
## config 2: defining the default collection that we write to
## config 3: defining spark jars packages to contain mongo-spark-connector and spark-kafka-connector

# ReadStream from kafka
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", topic2)\
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

def write_df_to_mongo(df, epoch_id):
    df.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .mode("append") \
        .save()

stream_to_mongo = df_realtime_prices \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_df_to_mongo) \
    .option("checkpointLocation", "/user/naya/checkpoint_mongo1") \
    .start()


stream_to_mongo.awaitTermination()


