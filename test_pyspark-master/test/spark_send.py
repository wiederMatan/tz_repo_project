from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pymongo
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,TimestampType, DoubleType, StringType, StructField
import os
from pyspark import SparkContext, SparkConf
import json

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

spark = SparkSession.builder\
    .appName('NayaProject')\
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/tstdb.statistics")\
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/tstdb.statistics")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3')\
    .getOrCreate()

# Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("name", StringType()),
    StructField("address", StringType())
])

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "cnt7-naya-cdh63:9092") \
    .option("subscribe", "T_INFO") \
    .load()

# Parse the value column as JSON and select the fields from the schema
parsed_df = df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.name", "data.address")

# Write the parsed data to MongoDB using the foreachBatch function
def write_to_mongodb(batch_df, batch_id):
    # Connect to the MongoDB database
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    # Select the database and collection
    db = client["mydatabase"]
    col = db["mycollection"]
    # Insert the batch DataFrame into MongoDB
    records = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
    for record in records:
        col.insert_one(record)

# Write the parsed data to MongoDB for each batch
parsed_df \
    .writeStream \
    .foreachBatch(write_to_mongodb) \
    .start() \
    .awaitTermination()
