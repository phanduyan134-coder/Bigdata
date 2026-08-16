"""Train 24 direct Spark MLlib models for PM2.5 forecasting horizons."""
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql.functions import col, lead, row_number

from src.air_quality import FEATURES, get_spark, prepare_spark_data

MODEL_ROOT = Path(__file__).resolve().parent / "models" / "pm25_multi_horizon_spark"


def main():
    spark = get_spark()
    data = prepare_spark_data(spark).drop("target_pm25")
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")

    for horizon in range(1, 25):
        window = Window.partitionBy("location_id").orderBy("timestamp")
        training_data = data.withColumn("target", lead("pm25", horizon).over(window)).dropna(subset=["target"])
        ranks = training_data.withColumn("rank", row_number().over(Window.partitionBy("location_id").orderBy("timestamp")))
        counts = ranks.groupBy("location_id").count().withColumnRenamed("count", "location_count")
        train = ranks.join(counts, "location_id").filter(col("rank") <= col("location_count") * 0.8)
        estimator = RandomForestRegressor(featuresCol="features", labelCol="target", numTrees=20, maxDepth=10, maxBins=32, seed=42)
        Pipeline(stages=[assembler, estimator]).fit(train).write().overwrite().save(str(MODEL_ROOT / f"h{horizon}"))
        print(f"Saved direct {horizon}-hour model")

    spark.stop()


if __name__ == "__main__":
    main()
