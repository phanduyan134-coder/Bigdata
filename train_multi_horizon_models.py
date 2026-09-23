"""Huấn luyện bộ 24 mô hình Random Forest phân tán theo chiến lược Direct Multi-horizon.

Module này thực hiện các kỹ thuật Machine Learning phân tán cốt lõi trên Apache Spark MLlib:
1. Áp dụng chiến lược Trực tiếp (Direct Multi-horizon Forecasting) thay vì Đệ quy (Recursive)
   nhằm triệt tiêu hoàn toàn hiện tượng bùng nổ sai số tích lũy (Error Propagation) trong dự báo dài hạn.
2. Sử dụng hàm lead("pm25", horizon) trên cửa sổ phân vùng Spark Window để gán nhãn nồng độ bụi
   tương ứng với đúng từng mốc thời gian từ h=1 đến h=24 giờ tới.
3. Gom 13 cột đặc trưng thành VectorAssembler chuẩn và huấn luyện song song 24 mô hình Random Forest
   (mỗi mô hình gồm 20 cây quyết định, độ sâu 10 tầng) trực tiếp trên bộ nhớ RAM.
4. Đóng gói và lưu trữ 24 PipelineModel riêng biệt từ h1 đến h24 phục vụ suy luận thời gian thực.
"""

from pathlib import Path
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql.functions import col, lead, row_number

from src.air_quality import FEATURES, get_spark, prepare_spark_data

# Thư mục gốc lưu trữ toàn bộ 24 mô hình đã huấn luyện
MODEL_ROOT = Path(__file__).resolve().parent / "models" / "pm25_multi_horizon_spark"


def main():
    """Quy trình huấn luyện phân tán độc lập cho 24 mốc dự báo tương lai (h = 1..24 giờ)."""
    print("=" * 80)
    print("🚀 BẮT ĐẦU HUẤN LUYỆN 24 MÔ HÌNH DIRECT MULTI-HORIZON TRÊN SPARK MLLIB")
    print("=" * 80)

    # Bước 1: Khởi tạo SparkSession và nạp tập dữ liệu đã qua kỹ nghệ đặc trưng
    spark = get_spark()
    data = prepare_spark_data(spark).drop("target_pm25")

    # Bước 2: Khởi tạo VectorAssembler gom 13 đặc trưng vào một Feature Vector duy nhất
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")

    # Bước 3: Vòng lặp huấn luyện độc lập cho từng mốc thời gian horizon từ 1 đến 24 giờ
    # Điểm đột phá thuật toán: Mỗi mốc horizon h có một mô hình riêng biệt học cách ánh xạ
    # trực tiếp từ đặc trưng hiện tại t sang giá trị nồng độ bụi tại thời điểm t + h,
    # giúp triệt tiêu hoàn toàn sự phụ thuộc đệ quy giữa các bước dự báo.
    for horizon in range(1, 25):
        # Định nghĩa cửa sổ phân vùng theo địa phương và sắp xếp theo thời gian
        window = Window.partitionBy("location_id").orderBy("timestamp")

        # Tạo nhãn mục tiêu cho mốc h giờ trong tương lai bằng hàm lead()
        training_data = data.withColumn(
            "target", lead("pm25", horizon).over(window)
        ).dropna(subset=["target"])

        # Phân chia tập dữ liệu theo thứ tự thời gian thực (80% Train / 20% Test)
        ranks = training_data.withColumn(
            "rank", row_number().over(Window.partitionBy("location_id").orderBy("timestamp"))
        )
        counts = ranks.groupBy("location_id").count().withColumnRenamed("count", "location_count")
        train = ranks.join(counts, "location_id").filter(col("rank") <= col("location_count") * 0.8)

        # Cấu hình thuật toán Random Forest Regressor
        estimator = RandomForestRegressor(
            featuresCol="features",
            labelCol="target",
            numTrees=20,       # 20 cây quyết định phân tán
            maxDepth=10,       # Độ sâu tối đa 10 tầng để chống học vẹt (Overfitting)
            maxBins=32,        # Số thùng phân chia đặc trưng
            seed=42            # Đảm bảo tính nhất quán và khả năng tái lập
        )

        # Đóng gói Pipeline và tiến hành huấn luyện song song
        pipeline = Pipeline(stages=[assembler, estimator])
        model = pipeline.fit(train)

        # Lưu trữ mô hình tương ứng với horizon h ra thư mục đĩa cứng
        output_dir = MODEL_ROOT / f"h{horizon}"
        model.write().overwrite().save(str(output_dir))
        print(f"[✓ Hoàn thành {horizon:02d}/24] Đã huấn luyện và lưu mô hình dự báo sau {horizon:02d} giờ tại: {output_dir.name}")

    print("=" * 80)
    print("🎉 TOÀN BỘ 24 MÔ HÌNH MULTI-HORIZON ĐÃ ĐƯỢC HUẤN LUYỆN VÀ LƯU TRỮ THÀNH CÔNG!")
    print("=" * 80)

    # Giải phóng tài nguyên tính toán của Spark
    spark.stop()


if __name__ == "__main__":
    main()
