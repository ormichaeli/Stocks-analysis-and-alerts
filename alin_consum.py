from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import *

from pyspark import SparkConf
#================== integrate wth kafka======================================================#
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
#================== connection between  spark and kafka=======================================#
#==============================================================================================
access_key="AKIA5V5BUXHLZKRSMGPK"
secret_key="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"
endpoint="s3.amazonaws.com"

conf = SparkConf()
conf.set("fs.s3a.access.key", access_key)
conf.set("fs.s3a.secret.key", secret_key)
conf.set("fs.s3a.endpoint", endpoint)
# Create a SparkSession
spark = SparkSession \
        .builder \
        .appName("KafkaToS3") \
        .config(conf=conf)\
        .getOrCreate()

#==============================================================================================
#=========================================== ReadStream from kafka===========================#
socketDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cnt7-naya-cdh63:9092") \
    .option("Subscribe", "From_Kafka_To_Spark_s3")\
    .load()\
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
#==============================================================================================
#==============================Create schema for create df from json=========================#
schema = t.StructType() \
    .add("vendorid", t.StringType())                    .add("lpep_pickup_datetime", t.StringType()) \
    .add("lpep_dropoff_datetime", t.StringType())       .add("store_and_fwd_flag", t.StringType()) \
    .add("ratecodeid", t.StringType())                  .add("pickup_longitude", t.StringType()) \
    .add("pickup_latitude", t.StringType())             .add("dropoff_longitude", t.StringType()) \
    .add("dropoff_latitude", t.StringType())            .add("passenger_count", t.StringType()) \
    .add("trip_distance", t.StringType())               .add("fare_amount", t.StringType()) \
    .add("extra", t.StringType())                       .add("mta_tax", t.StringType()) \
    .add("tip_amount", t.StringType())                  .add("tolls_amount", t.StringType()) \
    .add("improvement_surcharge", t.StringType())       .add("total_amount", t.StringType())\
    .add("payment_type", t.StringType())                .add("trip_type", t.StringType())

#==============================================================================================
#==========================change json to dataframe with schema==============================#
taxiTripsDF = socketDF.select(f.col("value").cast("string")).select(f.from_json(f.col("value"), schema).alias("value")).select("value.*")

#====# 1: Remove spaces from column names====================================================#
taxiTripsDF = taxiTripsDF \
    .withColumnRenamed("vendorid", "vendorid")                          .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")     .withColumnRenamed("passenger_count", "passenger_count") \
    .withColumnRenamed("trip_distance", "trip_distance")                .withColumnRenamed("ratecodeid", "ratecodeid") \
    .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")      .withColumnRenamed("payment_type", "PaymentType")


#============================================================================================#
#==== 3: Add date columns from timestamp=====================================================#
#============================================================================================#
taxiTripsDF = taxiTripsDF.withColumn('TripStartDT', taxiTripsDF['pickup_datetime'].cast('date'))
taxiTripsDF = taxiTripsDF.withColumn('TripEndDT', taxiTripsDF['dropoff_datetime'].cast('date'))

#============================================================================================#
#==== 4: Add/convert/casting Additional columns types=========================================#
#============================================================================================#
taxiTripsDF = taxiTripsDF\
    .withColumn('trip_distance', taxiTripsDF['trip_distance'].cast('double'))\
    .withColumn('pickup_longitude', taxiTripsDF['pickup_longitude'].cast('double')) \
    .withColumn('pickup_latitude', taxiTripsDF['pickup_latitude'].cast('double')) \
    .withColumn('dropoff_longitude', taxiTripsDF['dropoff_longitude'].cast('double')) \
    .withColumn('dropoff_latitude', taxiTripsDF['dropoff_latitude'].cast('double'))

taxiTripsDF = taxiTripsDF\
    .withColumn("hourd", hour(taxiTripsDF["pickup_datetime"])) \
    .withColumn("minuted", minute(taxiTripsDF["pickup_datetime"])) \
    .withColumn("secondd", second(taxiTripsDF["pickup_datetime"]))\
    .withColumn("dayofweek", dayofweek(taxiTripsDF["TripStartDT"])) \
    .withColumn("week_day_full", date_format(col("TripStartDT"), "EEEE"))

## CREATE A CLEANED DATA-FRAME BY DROPPING SOME UN-NECESSARY COLUMNS & FILTERING FOR UNDESIRED VALUES OR OUTLIERS
taxiTripsDF = taxiTripsDF\
    .drop('store_and_fwd_flag','fare_amount','extra','tolls_amount','mta_tax','improvement_surcharge','trip_type','ratecodeid','pickup_datetime','dropoff_datetime','PaymentType','TripStartDT','TripEndDT')\
    .filter("passenger_count > 0 and passenger_count < 8  AND tip_amount >= 0 AND tip_amount < 30 AND fare_amount >= 1 AND fare_amount < 150 AND trip_distance > 0 AND trip_distance < 100 ")


taxiTripsDF.printSchema()



hdfs_query = taxiTripsDF.writeStream.format("csv") \
    .option("path", "s3a://realtime-stocks-prices-deproj/output2/") \
    .option("checkpointLocation", "s3a://realtime-stocks-prices-deproj/checkpoint2/") \
    .start()






hdfs_query.awaitTermination()