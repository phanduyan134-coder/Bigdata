"""Create direct 1-24 hour PM2.5 forecasts from the realtime Parquet stream."""
from pathlib import Path
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, expr, hour, lag, when

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.air_quality import FEATURES

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "streaming" / "air_quality"
OUTPUT_PATH = ROOT / "data" / "streaming" / "multi_horizon_predictions"
CHECKPOINT = ROOT / "data" / "streaming" / "multi_horizon_prediction_checkpoint"
MODEL_ROOT = ROOT / "models" / "pm25_multi_horizon_spark"

spark = (
    SparkSession.builder.appName("AirQualityMultiHorizonForecast")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
models = {horizon: PipelineModel.load(str(MODEL_ROOT / f"h{horizon}")) for horizon in range(1, 25)}


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
    history = spark.read.parquet(str(RAW_PATH)).dropDuplicates(["location_id", "timestamp"])
    feature_data = add_features(history).dropna(subset=FEATURES)
    keys = batch.select("location_id", "timestamp").dropDuplicates()
    forecasts = []
    for horizon, model in models.items():
        forecasts.append(
            model.transform(feature_data).join(keys, ["location_id", "timestamp"])
            .select("location_id", "location_name", "timestamp", "pm25", "aqi", "prediction")
            .withColumnRenamed("prediction", "predicted_pm25")
            .withColumn("horizon_hours", expr(str(horizon)))
            .withColumn("forecast_for", expr(f"timestamp + INTERVAL {horizon} HOURS"))
        )
    result = forecasts[0]
    for forecast in forecasts[1:]:
        result = result.unionByName(forecast)
    if not result.rdd.isEmpty():
        result.write.mode("append").parquet(str(OUTPUT_PATH))


stream = spark.readStream.schema(spark.read.parquet(str(RAW_PATH)).schema).parquet(str(RAW_PATH))
query = stream.writeStream.foreachBatch(forecast_batch).option("checkpointLocation", str(CHECKPOINT)).start()
query.awaitTermination()
