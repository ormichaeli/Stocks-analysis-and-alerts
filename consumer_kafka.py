from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, FloatType, IntegerType


bootstrapServers = "Cnt7-naya-cdh63:9092"
# for reading from kafka
topic1 = "stocks_prices_kafka"

# for writing to kafka
topic4 = "users_emails"

spark = SparkSession \
    .builder \
    .appName("real_time_prices_kafka") \
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
    .option("subscribe", topic1) \
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

mongo_schema = StructType() \
    .add("_id", StringType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add('email_address', StringType()) \
    .add('stock_ticker', StringType()) \
    .add('price', StringType()) \
    .add('news', StringType()) \
    .add('is_active', IntegerType())

# getting all users from mongo into df.
### although we already defined our default collection that we read from,
### the command with option() is more dynamic- we can choose another path to read from
df_all_users = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/stocks_db.users").schema(mongo_schema).load()

df_relevant_users_for_the_stock = df_all_users.join(df_realtime_prices, df_realtime_prices['stock_ticker'] == df_all_users['stock_ticker'], 'inner')
df_users_to_send_emails = df_relevant_users_for_the_stock.filter(df_relevant_users_for_the_stock['is_active'] == 1) \
                            .filter(df_realtime_prices['current_price'] <= (df_relevant_users_for_the_stock['price']).cast(FloatType()))

# sending to kafka as json string
stream_to_kafka = df_users_to_send_emails \
    .selectExpr("CAST(_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("topic", topic4) \
    .option("checkpointLocation", 'checkpointLocation_kafka1') \
    .start()

stream_to_kafka.awaitTermination()