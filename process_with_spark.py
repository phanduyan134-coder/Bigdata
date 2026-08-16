from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


ROOT = Path(__file__).resolve().parent
INPUT = str(ROOT / "air+quality" / "AirQualityUCI.csv")
OUTPUT = str(ROOT / "data" / "processed_spark")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("AirQualityPreprocessing").getOrCreate()
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(INPUT)
    )
    cleaned = df.drop("", "_c15", "_c16", "_c17", "_c18", "_c19")
    cleaned = cleaned.withColumn(
        "timestamp", to_timestamp(
            col("Date") + " " + col("Time"), "dd/MM/yyyy HH.mm.ss"
        )
    )
    cleaned = cleaned.replace(-200, None)
    Path(OUTPUT).parent.mkdir(exist_ok=True)
    cleaned.write.mode("overwrite").parquet(OUTPUT)
    print(f"Wrote {cleaned.count()} rows to {OUTPUT}")
    spark.stop()
