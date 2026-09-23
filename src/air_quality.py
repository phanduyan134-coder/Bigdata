"""Module xử lý phân tán và huấn luyện mô hình dự báo chất lượng không khí trên Apache Spark.

Chức năng chính của module:
1. Khởi tạo và quản lý phiên làm việc SparkSession với cấu hình tối ưu bộ nhớ.
2. Kỹ nghệ đặc trưng chuỗi thời gian (Feature Engineering) và chống rò rỉ dữ liệu (Data Leakage).
3. Đóng gói Spark ML Pipeline và huấn luyện mô hình học máy Random Forest Regressor phân tán.
4. Lưu trữ và nạp mô hình PipelineModel phục vụ suy luận thời gian thực.
"""

from pathlib import Path
import sys
import pandas as pd
import pyspark
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, hour, lag, row_number, to_timestamp, when

# -----------------------------------------------------------------------------
# 1. CẤU HÌNH ĐƯỜNG DẪN VÀ DANH SÁCH 13 ĐẶC TRƯNG HỌC MÁY (FEATURES)
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
RAW_FILE = ROOT / "data" / "raw" / "multi_location_air_quality.csv"
MODEL_DIR = ROOT / "models" / "pm25_spark_model_v2"

# 13 đặc trưng cốt lõi đưa vào mô hình dự báo:
# - Nhóm thông số quan trắc gốc: pm25, pm10, co, no2, so2, o3, aqi
# - Nhóm biến trễ quá khứ: pm25_lag1 (1h trước), pm25_lag3 (3h trước)
# - Nhóm biến thống kê trượt: pm25_mean3 (trung bình trượt 3h trước)
# - Nhóm biến chu kỳ thời gian: hour (giờ trong ngày), day_of_week (ngày trong tuần)
# - Nhóm mã định danh địa lý: location_code (0: Cần Thơ, 1: Đà Nẵng, 2: Hà Nội, 3: TP.HCM)
FEATURES = [
    "pm25", "pm10", "co", "no2", "so2", "o3", "aqi",
    "pm25_lag1", "pm25_lag3", "pm25_mean3",
    "hour", "day_of_week", "location_code"
]


# -----------------------------------------------------------------------------
# 2. KHỞI TẠO VÀ QUẢN LÝ PHIÊN TÍNH TOÁN SPARK SESSION
# -----------------------------------------------------------------------------
def get_spark():
    """Khởi tạo phiên làm việc SparkSession với cấu hình tối ưu tài nguyên tính toán.

    Chi tiết cấu hình phân tán:
    - master("local[2]"): Khởi chạy Spark ở chế độ cục bộ tận dụng 2 lõi CPU.
    - driver.memory ("4g") & executor.memory ("4g"): Cấp phát 4GB RAM cho Driver và Executor để xử lý dữ liệu lớn mượt mà.
    - sql.shuffle.partitions ("8"): Giảm số phân vùng shuffle xuống 8 để tối ưu hiệu năng trên môi trường máy đơn.
    - Cơ chế dọn dẹp phiên: Tự động ngắt các context JVM bị treo để ngăn lỗi khi ứng dụng Streamlit reload.
    """
    import os
    python_exe = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exe)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exe)

    # Kiểm tra và dọn dẹp context cũ nếu phiên làm việc trước đó bị gián đoạn
    active = SparkSession.getActiveSession()
    if active is not None:
        try:
            if active.sparkContext._jsc.sc().isStopped():
                active.stop()
                SparkSession.clearActiveSession()
                SparkSession.clearDefaultSession()
                pyspark.SparkContext._active_spark_context = None
        except Exception:
            SparkSession.clearActiveSession()
            SparkSession.clearDefaultSession()
            pyspark.SparkContext._active_spark_context = None

    return (
        SparkSession.builder.appName("AirQualityForecast")
        .master("local[2]")                           # Sử dụng 2 lõi tính toán song song
        .config("spark.driver.memory", "4g")          # 4GB RAM cho Driver điều phối
        .config("spark.executor.memory", "4g")        # 4GB RAM cho Executor xử lý
        .config("spark.sql.shuffle.partitions", "8")  # Tối ưu hóa phân vùng khi shuffle
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# 3. ĐỌC DỮ LIỆU BẢNG BẰNG PANDAS
# -----------------------------------------------------------------------------
def load_data(path=RAW_FILE):
    """Đọc dữ liệu từ file CSV phục vụ cho giao diện hiển thị nhanh trên Dashboard."""
    return pd.read_csv(path)


# -----------------------------------------------------------------------------
# 4. KỸ NGHỆ ĐẶC TRƯNG CHUỖI THỜI GIAN & CHỐNG RÒ RỈ DỮ LIỆU (DATA LEAKAGE)
# -----------------------------------------------------------------------------
def prepare_spark_data(spark, path=RAW_FILE):
    """Tiền xử lý dữ liệu, chuẩn hóa schema tĩnh và trích xuất đặc trưng chuỗi thời gian.

    Quy trình kỹ thuật gồm 8 bước:
    - Bước 1: Nạp dữ liệu CSV vào Spark DataFrame và ép kiểu số thực (DoubleType) cho các cột cảm biến.
    - Bước 2: Chuyển đổi chuỗi thời gian sang định dạng Timestamp chuẩn ISO 8601.
    - Bước 3: Mã hóa tên địa phương thành mã số nguyên (location_code).
    - Bước 4: Thiết lập cửa sổ phân vùng Window.partitionBy("location_id").orderBy("timestamp")
              nhằm cô lập dữ liệu theo từng trạm quan trắc, triệt tiêu 100% rủi ro rò rỉ dữ liệu (Data Leakage)
              giữa các thành phố cũng như giữa quá khứ và tương lai.
    - Bước 5: Trích xuất các biến trễ quá khứ (pm25_lag1, pm25_lag3) để nắm bắt quán tính ô nhiễm.
    - Bước 6: Trích xuất biến trung bình trượt 3 giờ (pm25_mean3) để khử nhiễu đột biến ngắn hạn.
    - Bước 7: Trích xuất biến chu kỳ đô thị (giờ trong ngày, ngày trong tuần).
    - Bước 8: Tạo nhãn mục tiêu cần dự báo ở bước kế tiếp (target_pm25 tại t+1) và loại bỏ dòng null.
    """
    # Bước 1: Đọc tệp CSV nguồn
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(path))

    # Bước 2: Ép kiểu dữ liệu nghiêm ngặt cho các chỉ số môi trường
    numeric = ["latitude", "longitude", "pm25", "pm10", "co", "no2", "so2", "o3", "aqi"]
    for name in numeric:
        df = df.withColumn(name, col(name).cast("double"))
    df = df.withColumn("timestamp", to_timestamp("timestamp"))

    # Bước 3: Mã hóa mã địa lý đô thị
    df = df.withColumn(
        "location_code",
        when(col("location_id") == "cantho", 0.0)
        .when(col("location_id") == "danang", 1.0)
        .when(col("location_id") == "hanoi", 2.0)
        .otherwise(3.0)  # TP.HCM = 3.0
    )

    # Bước 4: Định nghĩa cửa sổ không gian - thời gian chống Data Leakage
    window = Window.partitionBy("location_id").orderBy("timestamp")

    # Bước 5: Trích xuất đặc trưng trễ quá khứ (Lags)
    df = df.withColumn("pm25_lag1", lag("pm25", 1).over(window))  # Nồng độ bụi 1 giờ trước
    df = df.withColumn("pm25_lag3", lag("pm25", 3).over(window))  # Nồng độ bụi 3 giờ trước

    # Bước 6: Trích xuất trung bình trượt 3 giờ trước (Rolling Mean)
    preceding = window.rowsBetween(-3, -1)
    df = df.withColumn("pm25_mean3", avg("pm25").over(preceding))

    # Bước 7: Trích xuất biến chu kỳ sinh hoạt đô thị
    df = df.withColumn("hour", hour("timestamp").cast("double"))
    df = df.withColumn("day_of_week", ((col("timestamp").cast("long") / 86400 + 4) % 7).cast("double"))

    # Bước 8: Tạo nhãn mục tiêu cần dự báo tại t+1 (dịch chuyển nhãn về phía trước)
    df = df.withColumn("target_pm25", lag("pm25", -1).over(window))

    # Loại bỏ các dòng trống do dịch chuyển trễ ở đầu chuỗi
    return df.dropna(subset=FEATURES + ["target_pm25"])


# -----------------------------------------------------------------------------
# 5. QUY TRÌNH HUẤN LUYỆN MÔ HÌNH RANDOM FOREST TRÊN SPARK MLLIB
# -----------------------------------------------------------------------------
def train_model():
    """Quy trình huấn luyện mô hình học tập hợp Random Forest Regressor phân tán.

    Các bước triển khai:
    - Gom 13 cột đặc trưng thành một Feature Vector duy nhất bằng VectorAssembler.
    - Cấu hình thuật toán Random Forest: 20 cây quyết định (numTrees=20), độ sâu tối đa 10 (maxDepth=10),
      giúp học tốt các mối quan hệ phi tuyến phức tạp của khí tượng và chống học vẹt (Overfitting).
    - Đóng gói quy trình tiền xử lý và mô hình vào một Spark ML Pipeline hoàn chỉnh.
    - Phân chia tập dữ liệu theo đúng trình tự thời gian (Time-series Split): 80% Train, 20% Test.
    - Huấn luyện song song trực tiếp trên bộ nhớ RAM của cụm Spark.
    - Đánh giá độ chính xác thực nghiệm qua các chỉ số MAE và RMSE trên tập kiểm thử độc lập.
    - Xuất mô hình hoàn chỉnh ra đĩa cứng dưới định dạng PipelineModel.
    """
    spark = get_spark()
    data = prepare_spark_data(spark)

    # 1. Gom các cột đặc trưng thành Vector duy nhất theo chuẩn Spark MLlib
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")

    # 2. Khởi tạo mô hình Random Forest Regressor với các tham số tối ưu
    model = RandomForestRegressor(
        featuresCol="features",
        labelCol="target_pm25",
        numTrees=20,               # 20 cây quyết định phân tán
        maxDepth=10,               # Độ sâu tối đa 10 tầng để chống Overfitting
        maxBins=32,                # Số lượng thùng phân chia đặc trưng
        minInstancesPerNode=2,     # Số mẫu tối thiểu trên mỗi nút lá
        seed=42                    # Cố định seed đảm bảo tính tái lập kết quả
    )

    # 3. Đóng gói vào Spark ML Pipeline thống nhất
    pipeline = Pipeline(stages=[assembler, model])

    # 4. Phân chia Train/Test theo thứ tự thời gian (80% Train / 20% Test độc lập)
    ranks = data.withColumn("rank", row_number().over(Window.partitionBy("location_id").orderBy("timestamp")))
    counts = ranks.groupBy("location_id").count().withColumnRenamed("count", "location_count")
    ranked = ranks.join(counts, "location_id").withColumn("is_train", col("rank") <= col("location_count") * 0.8)

    train = ranked.filter(col("is_train") == True)   # 80% dữ liệu quá khứ dùng huấn luyện
    test = ranked.filter(col("is_train") == False)   # 20% dữ liệu mới nhất dùng kiểm thử

    # 5. Huấn luyện phân tán trên Spark
    fitted = pipeline.fit(train)

    # 6. Đánh giá sai số trên tập kiểm thử độc lập
    predictions = fitted.transform(test)
    errors = predictions.selectExpr("target_pm25 - prediction as error", "prediction", "target_pm25").toPandas()
    mae = float(errors["error"].abs().mean())
    rmse = float((errors["error"] ** 2).mean() ** 0.5)

    # 7. Lưu trữ mô hình đã huấn luyện bền vững lên đĩa cứng
    fitted.write().overwrite().save(str(MODEL_DIR))

    metrics = {
        "mae": mae,
        "rmse": rmse,
        "rows": data.count(),
        "test_rows": test.count(),
        "locations": data.select("location_id").distinct().count(),
        "engine": "Apache Spark MLlib"
    }
    spark.stop()
    return metrics


# -----------------------------------------------------------------------------
# 6. TẢI MÔ HÌNH PHỤC VỤ SUY LUẬN THỜI GIAN THỰC (INFERENCE)
# -----------------------------------------------------------------------------
def load_model(spark=None):
    """Nạp PipelineModel đã lưu từ đĩa cứng lên bộ nhớ để suy luận dự báo tức thời."""
    session = spark or get_spark()
    if not MODEL_DIR.exists():
        train_model()
    return PipelineModel.load(str(MODEL_DIR)), session
