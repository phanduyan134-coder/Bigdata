from pathlib import Path

import pandas as pd
import pyspark
import sys
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, hour, lag, row_number, to_timestamp, when

ROOT = Path(__file__).resolve().parents[1]
RAW_FILE = ROOT / "data" / "raw" / "multi_location_air_quality.csv"
MODEL_DIR = ROOT / "models" / "pm25_spark_model_v2"
FEATURES = ["pm25", "pm10", "co", "no2", "so2", "o3", "aqi", "pm25_lag1", "pm25_lag3", "pm25_mean3", "hour", "day_of_week", "location_code"]


def get_spark():
    import os
    python_exe = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exe)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exe)
    active = SparkSession.getActiveSession()
    if active is not None:
        try:
            if active.sparkContext._jsc.sc().isStopped():
                active.stop()
                SparkSession.clearActiveSession()
                SparkSession.clearDefaultSession()
                pyspark.SparkContext._active_spark_context = None
        except Exception:
            # A partially stopped JVM must not be reused by a Streamlit rerun.
            SparkSession.clearActiveSession()
            SparkSession.clearDefaultSession()
            pyspark.SparkContext._active_spark_context = None
    return (
        SparkSession.builder.appName("AirQualityForecast")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_data(path=RAW_FILE):
    return pd.read_csv(path)


def prepare_spark_data(spark, path=RAW_FILE):
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    numeric = ["latitude", "longitude", "pm25", "pm10", "co", "no2", "so2", "o3", "aqi"]
    for name in numeric:
        df = df.withColumn(name, col(name).cast("double"))
    df = df.withColumn("timestamp", to_timestamp("timestamp"))
    df = df.withColumn("location_code", when(col("location_id") == "cantho", 0.0).when(col("location_id") == "danang", 1.0).when(col("location_id") == "hanoi", 2.0).otherwise(3.0))
    window = Window.partitionBy("location_id").orderBy("timestamp")
    df = df.withColumn("pm25_lag1", lag("pm25", 1).over(window))
    df = df.withColumn("pm25_lag3", lag("pm25", 3).over(window))
    preceding = window.rowsBetween(-3, -1)
    df = df.withColumn("pm25_mean3", avg("pm25").over(preceding))
    df = df.withColumn("hour", hour("timestamp").cast("double"))
    df = df.withColumn("day_of_week", ((col("timestamp").cast("long") / 86400 + 4) % 7).cast("double"))
    df = df.withColumn("target_pm25", lag("pm25", -1).over(window))
    return df.dropna(subset=FEATURES + ["target_pm25"])


def train_model():
    spark = get_spark()
    data = prepare_spark_data(spark)
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
    model = RandomForestRegressor(featuresCol="features", labelCol="target_pm25", numTrees=20, maxDepth=10, maxBins=32, minInstancesPerNode=2, seed=42)
    pipeline = Pipeline(stages=[assembler, model])
    ranks = data.withColumn("rank", row_number().over(Window.partitionBy("location_id").orderBy("timestamp")))
    counts = ranks.groupBy("location_id").count().withColumnRenamed("count", "location_count")
    ranked = ranks.join(counts, "location_id").withColumn("is_train", col("rank") <= col("location_count") * 0.8)
    train = ranked.filter(col("is_train") == True)
    test = ranked.filter(col("is_train") == False)
    fitted = pipeline.fit(train)
    predictions = fitted.transform(test)
    errors = predictions.selectExpr("target_pm25 - prediction as error", "prediction", "target_pm25").toPandas()
    mae = float(errors["error"].abs().mean())
    rmse = float((errors["error"] ** 2).mean() ** 0.5)
    fitted.write().overwrite().save(str(MODEL_DIR))
    metrics = {"mae": mae, "rmse": rmse, "rows": data.count(), "test_rows": test.count(), "locations": data.select("location_id").distinct().count(), "engine": "Apache Spark MLlib"}
    spark.stop()
    return metrics


def load_model(spark=None):
    session = spark or get_spark()
    if not MODEL_DIR.exists():
        train_model()
    return PipelineModel.load(str(MODEL_DIR)), session
