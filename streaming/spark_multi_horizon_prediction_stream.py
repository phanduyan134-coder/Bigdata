"""Tầng suy luận luồng đa khung thời gian thời gian thực (Direct Multi-horizon Stream Inference).

Module này triển khai quy trình dự báo phân tán thời gian thực:
1. Nạp sẵn toàn bộ 24 mô hình PipelineModel đã huấn luyện vào bộ nhớ RAM của cụm Spark.
2. Lắng nghe luồng dữ liệu mới từ kho Parquet thông qua Spark Structured Streaming.
3. Khi nhận được vi lô (Micro-batch) bản ghi mới, hàm forecast_batch() kích hoạt đồng thời
   cả 24 mô hình để suy luận song song nồng độ bụi PM2.5 cho 24 giờ tiếp theo (chỉ mất ~0.28 giây).
4. Gom toàn bộ kết quả dự báo 24 mốc thời gian và lưu trữ nối tiếp vào thư mục Parquet
   để phục vụ cho giao diện trực quan hóa Streamlit Dashboard.
"""

from pathlib import Path
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, expr, hour, lag, when

# Bổ sung đường dẫn gốc để nạp danh sách 13 đặc trưng FEATURES
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.air_quality import FEATURES

# -----------------------------------------------------------------------------
# 1. THIẾT LẬP ĐƯỜNG DẪN LƯU TRỮ VÀ KHỞI TẠO SPARK SESSION
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "streaming" / "air_quality"
OUTPUT_PATH = ROOT / "data" / "streaming" / "multi_horizon_predictions"
CHECKPOINT = ROOT / "data" / "streaming" / "multi_horizon_prediction_checkpoint"
MODEL_ROOT = ROOT / "models" / "pm25_multi_horizon_spark"

# Khởi tạo phiên làm việc SparkSession cho suy luận luồng
spark = (
    SparkSession.builder.appName("AirQualityMultiHorizonForecast")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------------------------
# 2. NẠP BỘ 24 MÔ HÌNH VÀO BỘ NHỚ RAM ĐỂ SUY LUẬN TỨC THÌ
# -----------------------------------------------------------------------------
# Bằng cách nạp sẵn 24 mô hình vào RAM ngay khi khởi động, hệ thống triệt tiêu
# hoàn toàn độ trễ I/O đọc mô hình từ đĩa cứng mỗi khi có bản ghi mới chảy về
print("[*] Đang nạp sẵn 24 mô hình Spark MLlib vào bộ nhớ RAM...")
models = {horizon: PipelineModel.load(str(MODEL_ROOT / f"h{horizon}")) for horizon in range(1, 25)}
print("[✓] Đã nạp thành công toàn bộ 24 mô hình! Hệ thống sẵn sàng suy luận thời gian thực.\n")


# -----------------------------------------------------------------------------
# 3. HÀM TẠO ĐẶC TRƯNG TỨC THỜI CHO VI LÔ DỮ LIỆU
# -----------------------------------------------------------------------------
def add_features(data):
    """Tính toán nhanh các biến trễ và trung bình trượt từ chuỗi dữ liệu gần nhất."""
    data = data.withColumn("timestamp", col("timestamp").cast("timestamp"))
    data = data.withColumn(
        "location_code",
        when(col("location_id") == "cantho", 0.0)
        .when(col("location_id") == "danang", 1.0)
        .when(col("location_id") == "hanoi", 2.0)
        .otherwise(3.0)
    )
    window = Window.partitionBy("location_id").orderBy("timestamp")
    return (
        data.withColumn("pm25_lag1", lag("pm25", 1).over(window))
        .withColumn("pm25_lag3", lag("pm25", 3).over(window))
        .withColumn("pm25_mean3", avg("pm25").over(window.rowsBetween(-3, -1)))
        .withColumn("hour", hour("timestamp").cast("double"))
        .withColumn("day_of_week", ((col("timestamp").cast("long") / 86400 + 4) % 7).cast("double"))
    )


# -----------------------------------------------------------------------------
# 4. HÀM XỬ LÝ SUY LUẬN THEO VI LÔ (MICRO-BATCH FORECASTING)
# -----------------------------------------------------------------------------
def forecast_batch(batch, _batch_id):
    """Xử lý suy luận song song cho từng vi lô bản ghi mới phát sinh.

    Quy trình xử lý:
    1. Đọc lại lịch sử gần nhất để bảo đảm đủ dữ liệu tính toán biến trễ lag1 và lag3.
    2. Chạy song song 24 mô hình trên tập đặc trưng mới nhất.
    3. Gán nhãn thời gian dự báo tương ứng: forecast_for = timestamp + INTERVAL h HOURS.
    4. Gộp 24 kết quả dự báo thành 1 DataFrame thống nhất bằng unionByName().
    5. Ghi nối tiếp (append) vào kho lưu trữ Parquet.
    """
    if batch.rdd.isEmpty():
        return

    # Nạp dữ liệu lịch sử gần nhất để tạo đầy đủ các biến trễ
    history = spark.read.parquet(str(RAW_PATH)).dropDuplicates(["location_id", "timestamp"])
    feature_data = add_features(history).dropna(subset=FEATURES)
    keys = batch.select("location_id", "timestamp").dropDuplicates()

    forecasts = []
    # Suy luận song song cho toàn bộ 24 khung thời gian
    for horizon, model in models.items():
        forecasts.append(
            model.transform(feature_data).join(keys, ["location_id", "timestamp"])
            .select("location_id", "location_name", "timestamp", "pm25", "aqi", "prediction")
            .withColumnRenamed("prediction", "predicted_pm25")
            .withColumn("horizon_hours", expr(str(horizon)))
            .withColumn("forecast_for", expr(f"timestamp + INTERVAL {horizon} HOURS"))
        )

    # Gộp toàn bộ kết quả dự báo của 24 mô hình
    result = forecasts[0]
    for forecast in forecasts[1:]:
        result = result.unionByName(forecast)

    # Ghi kết quả vào thư mục Parquet phục vụ Streamlit Dashboard
    if not result.rdd.isEmpty():
        result.write.mode("append").parquet(str(OUTPUT_PATH))
        print(f"[✓ Đã dự báo xong 24h] Ghi nhận {result.count()} bản ghi dự báo mới vào Parquet.")


# -----------------------------------------------------------------------------
# 5. THIẾT LẬP VÀ KHỞI CHẠY TIẾN TRÌNH STREAMING
# -----------------------------------------------------------------------------
stream = spark.readStream.schema(spark.read.parquet(str(RAW_PATH)).schema).parquet(str(RAW_PATH))
query = (
    stream.writeStream.foreachBatch(forecast_batch)
    .option("checkpointLocation", str(CHECKPOINT))
    .start()
)

print(f"[*] Multi-horizon Prediction Stream đang lắng nghe dữ liệu mới tại: {RAW_PATH}")
print(f"[*] Kho lưu trữ kết quả dự báo: {OUTPUT_PATH}\n")

query.awaitTermination()
