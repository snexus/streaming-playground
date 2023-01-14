import findspark

findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, udf, lit, expr


from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json
import ast

# import org.apache.spark.sql.functions.from_json

BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "sensor_events_json"
OUTPUT_PATH = "/home/snexus/projects/personal/streaming-playground/processing/output"

spark = SparkSession.builder.appName("TestApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("sensor_reading", StringType(), True),
    ]
)


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

df.printSchema()

query = (
    df
    # The following offset of 6 is required due to Confluent Kafka's Schema Registry
    # See https://blogit.michelin.io/kafka-to-delta-lake-using-apache-spark-streaming-avro/
    .select(expr("substring(value, 6, length(value)-5)").alias("json_obj"))
    .selectExpr("CAST(json_obj AS STRING) as json_string")
    .withColumn(
        "parsed_value", from_json(col("json_string"), schema, {"mode": "PERMISSIVE"})
    )
    .withColumn("sensor_id", col("parsed_value.sensor_id"))
    .withColumn("sensor_type", col("parsed_value.sensor_type"))
    .withColumn("sensor_reading", col("parsed_value.sensor_reading"))
    .writeStream.format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()
