"""Tầng xử lý dòng (Stream Processing) và Lưu trữ định dạng cột Apache Parquet.

Module này thực hiện các kỹ thuật Big Data cốt lõi:
1. Tiếp nhận luồng dữ liệu thời gian thực từ Apache Kafka bằng Spark Structured Streaming.
2. Ép kiểu schema tĩnh (Static Schema Enforcement) để kiểm soát chất lượng dữ liệu và loại bỏ bản ghi lỗi.
3. Bóc tách payload JSON phân tán với hàm from_json().
4. Ghi dữ liệu vi lô (Micro-batch) vào định dạng cột Apache Parquet nén Snappy.
5. Thiết lập thư mục Checkpoint và Write-Ahead Log (WAL) nhằm đảm bảo tính chịu lỗi
   và cam kết ngữ nghĩa xử lý chính xác một lần (Exactly-Once Processing).
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# -----------------------------------------------------------------------------
# 1. ĐỊNH NGHĨA ĐƯỜNG DẪN LƯU TRỮ VÀ THƯ MỤC CHECKPOINT
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]

# Thư mục đích lưu trữ tệp Parquet luồng (Data Sink)
OUTPUT = ROOT / "data" / "streaming" / "air_quality"

# Thư mục lưu trạng thái và nhật ký Write-Ahead Log bảo đảm tính chịu lỗi Exactly-Once
CHECKPOINT = ROOT / "data" / "streaming" / "checkpoint"

# -----------------------------------------------------------------------------
# 2. ĐỊNH NGHĨA SCHEMA TĨNH NGHIÊM NGẶT (STATIC SCHEMA ENFORCEMENT)
# -----------------------------------------------------------------------------
# Việc chỉ định cấu trúc Schema rõ ràng giúp tối ưu hóa hiệu năng phân tích cú pháp
# và loại bỏ hoàn toàn các trường dữ liệu dị thường trước khi lưu vào kho dữ liệu
schema = StructType([
    StructField("location_id", StringType(), True),
    StructField("location_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("aqi", DoubleType(), True),
])

# -----------------------------------------------------------------------------
# 3. KHỞI TẠO PHIÊN TÍNH TOÁN SPARK STRUCTURED STREAMING
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("AirQualityKafkaStreaming")
    .master("local[2]")                           # Cấu hình 2 lõi CPU xử lý song song
    .config("spark.sql.shuffle.partitions", "2")  # Tối ưu hóa phân vùng cho luồng nhẹ
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")            # Giới hạn mức log thông tin để theo dõi thuận tiện

# -----------------------------------------------------------------------------
# 4. KẾT NỐI VÀ ĐỌC LUỒNG DỮ LIỆU TỪ APACHE KAFKA (STREAM SOURCE)
# -----------------------------------------------------------------------------
# - format("kafka"): Tích hợp đầu nối Kafka Source chính thức của Apache Spark.
# - startingOffsets("latest"): Luôn bắt đầu đọc từ các thông điệp thời gian thực mới nhất trên topic.
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "air-quality")
    .option("startingOffsets", "latest")
    .load()
)

# Chuyển đổi payload byte array thành String và phân tích cú pháp theo StructType Schema
records = raw.select(
    from_json(col("value").cast("string"), schema).alias("record")
).select("record.*")

# -----------------------------------------------------------------------------
# 5. GHI LUỒNG DỮ LIỆU RA ĐỊNH DẠNG CỘT PARQUET (STREAM SINK & EXACTLY-ONCE)
# -----------------------------------------------------------------------------
# - outputMode("append"): Ghi nối tiếp các bản ghi mới phát sinh vào kho lưu trữ.
# - checkpointLocation: Lưu vết vị trí offset của Kafka đã xử lý vào đĩa cứng. Khi có sự cố mạng
#   hoặc máy chủ sập nguồn, Spark sẽ tự động phục hồi đúng offset cũ, cam kết Exactly-Once Processing.
# - trigger(processingTime="5 seconds"): Kích hoạt xử lý theo cơ chế vi lô (Micro-batch) mỗi 5 giây.
query = (
    records.writeStream.format("parquet")
    .outputMode("append")
    .option("path", str(OUTPUT))
    .option("checkpointLocation", str(CHECKPOINT))
    .trigger(processingTime="5 seconds")
    .start()
)

print(f"[*] Spark Structured Streaming đang hoạt động...")
print(f"[*] Tệp Parquet được lưu trữ dạng cột (nén Snappy) tại: {OUTPUT}")
print(f"[*] Metadata Checkpoint đảm bảo Exactly-Once tại: {CHECKPOINT}\n")

# Duy trì tiến trình streaming liên tục lắng nghe dữ liệu
query.awaitTermination()
