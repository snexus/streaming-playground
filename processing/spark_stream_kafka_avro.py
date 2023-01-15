import findspark

findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, udf, lit, expr

from pyspark.sql.avro.functions import from_avro

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
import os

# import org.apache.spark.sql.functions.from_json
dir_path = os.path.dirname(os.path.realpath(__file__))

BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "sensor_events_avro_timestamp"


AVRO_KEY_SCHEMA_PATH = os.path.join(
    dir_path, "reader_schema", "avro", "ValueSchema.avsc"
)
print(AVRO_KEY_SCHEMA_PATH)

JSON_SCHEMA_FOLDER = os.path.join(dir_path, "json_schema")


spark = SparkSession.builder.appName("TestApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# https://spark.apache.org/docs/3.0.0-preview/sql-data-sources-avro.html#to_avro-and-from_avro

event_schema = open(AVRO_KEY_SCHEMA_PATH, "r").read()
from_avro_options = {"mode": "PERMISSIVE"}

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

df.printSchema()
# selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
query = (
    df
    # The following offset of 6 is required due to Confluent Kafka's Schema Registry
    # See https://blogit.michelin.io/kafka-to-delta-lake-using-apache-spark-streaming-avro/
    .select(
        from_avro(
            expr("substring(value, 6, length(value)-5)"),
            event_schema,
            from_avro_options,
        ).alias("event")
    )
    .withColumn("sensor_id", col("event.sensor_id"))
    .withColumn("sensor_reading", col("event.sensor_reading"))
    .withColumn("sensor_type", col("event.sensor_type"))
    .withColumn("event_timestamp", ((col("event.event_timestamp") / 1e3).cast(TimestampType())))
    .writeStream.format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()
