from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("StockStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# --------------------------------------------------
# 2. Define the stock data schema
# --------------------------------------------------

stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", StringType(), True)
])


# --------------------------------------------------
# 3. Read streaming data from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "stock_prices")
    .option("startingOffsets", "latest")
    .load()
)


# --------------------------------------------------
# 4. Convert Kafka value from binary to JSON string
# --------------------------------------------------

json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value"
)


# --------------------------------------------------
# 5. Parse JSON into columns
# --------------------------------------------------

stock_df = (
    json_df
    .select(
        from_json(col("json_value"), stock_schema).alias("data")
    )
    .select("data.*")
)


# --------------------------------------------------
# 6. Write processed data to Parquet
# --------------------------------------------------

query = (
    stock_df
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "/app/data/stock_parquet"
    )
    .option(
        "checkpointLocation",
        "/app/data/stock_checkpoint"
    )
    .trigger(processingTime="10 seconds")
    .start()
)


# --------------------------------------------------
# 7. Keep streaming application running
# --------------------------------------------------

print("Spark streaming started...")
print("Writing processed stock data to /app/data/stock_parquet")

query.awaitTermination()