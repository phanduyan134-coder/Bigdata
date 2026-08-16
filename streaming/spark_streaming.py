"""Đọc Kafka bằng Spark Structured Streaming và ghi dữ liệu dạng Parquet."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "streaming" / "air_quality"
CHECKPOINT = ROOT / "data" / "streaming" / "checkpoint"

schema = StructType([
    StructField("location_id", StringType()),
    StructField("location_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("co", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("aqi", DoubleType()),
])

spark = (
    SparkSession.builder.appName("AirQualityKafkaStreaming")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "air-quality")
    .option("startingOffsets", "latest")
    .load()
)
records = raw.select(from_json(col("value").cast("string"), schema).alias("record")).select("record.*")

query = (
    records.writeStream.format("parquet")
    .outputMode("append")
    .option("path", str(OUTPUT))
    .option("checkpointLocation", str(CHECKPOINT))
    .trigger(processingTime="5 seconds")
    .start()
)
query.awaitTermination()
