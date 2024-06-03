from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("furniture", StringType(), True),
        StructField("color", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("ts", LongType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("DibimbingStreaming")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "dataeng-kafka:9092")
    .option("subscribe", "test-topic")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    stream_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("ts") / 1000))

windowDuration = "10 minutes"
slideDuration = "5 minutes"

running_total_df = (
    parsed_df.withWatermark("event_time", "5 minutes")
    .groupBy(window(col("event_time"), windowDuration, slideDuration))
    .agg(sum("price").alias("running_total_price"))
    .select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("running_total_price"),
    )
)

query = (
    running_total_df.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "1000")
    .start()
)


query.awaitTermination()
