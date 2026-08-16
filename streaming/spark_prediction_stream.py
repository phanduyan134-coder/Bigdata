"""Create one-hour PM2.5 forecasts from the Parquet stream using Spark MLlib."""
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, expr, hour, lag, when

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "streaming" / "air_quality"
OUTPUT_PATH = ROOT / "data" / "streaming" / "predictions"
CHECKPOINT = ROOT / "data" / "streaming" / "prediction_checkpoint"
MODEL_PATH = ROOT / "models" / "pm25_spark_model_v2"
FEATURES = ["pm25", "pm10", "co", "no2", "so2", "o3", "aqi", "pm25_lag1", "pm25_lag3", "pm25_mean3", "hour", "day_of_week", "location_code"]

spark = (
    SparkSession.builder.appName("AirQualityForecastStreaming")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
model = PipelineModel.load(str(MODEL_PATH))


def add_features(data):
    data = data.withColumn("timestamp", col("timestamp").cast("timestamp"))
    data = data.withColumn("location_code", when(col("location_id") == "cantho", 0.0).when(col("location_id") == "danang", 1.0).when(col("location_id") == "hanoi", 2.0).otherwise(3.0))
    window = Window.partitionBy("location_id").orderBy("timestamp")
    return (
        data.withColumn("pm25_lag1", lag("pm25", 1).over(window))
        .withColumn("pm25_lag3", lag("pm25", 3).over(window))
        .withColumn("pm25_mean3", avg("pm25").over(window.rowsBetween(-3, -1)))
        .withColumn("hour", hour("timestamp").cast("double"))
        .withColumn("day_of_week", ((col("timestamp").cast("long") / 86400 + 4) % 7).cast("double"))
    )


def forecast_batch(batch, _batch_id):
    if batch.rdd.isEmpty():
        return
    history = spark.read.parquet(str(RAW_PATH))
    data = history.dropDuplicates(["location_id", "timestamp"])
    feature_data = add_features(data).dropna(subset=FEATURES)
    keys = batch.select("location_id", "timestamp").dropDuplicates()
    forecasts = (
        model.transform(feature_data)
        .join(keys, ["location_id", "timestamp"])
        .select("location_id", "location_name", "timestamp", "pm25", "aqi", "prediction")
        .withColumnRenamed("prediction", "predicted_pm25")
        .withColumn("forecast_for", col("timestamp") + expr("INTERVAL 1 HOUR"))
    )
    if not forecasts.rdd.isEmpty():
        forecasts.write.mode("append").parquet(str(OUTPUT_PATH))


stream = spark.readStream.schema(spark.read.parquet(str(RAW_PATH)).schema).parquet(str(RAW_PATH))
query = (
    stream.writeStream.foreachBatch(forecast_batch)
    .option("checkpointLocation", str(CHECKPOINT))
    .start()
)
query.awaitTermination()
