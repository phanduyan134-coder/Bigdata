# Kafka/Spark Streaming local

## 1. Khởi động Kafka

```powershell
docker compose up -d
```

## 2. Cài Kafka client cho producer

```powershell
.\.venv\Scripts\python.exe -m pip install kafka-python
```

## 3. Chạy Spark Streaming

Mở PowerShell thứ nhất:

```powershell
\.venv\Scripts\python.exe streaming\spark_streaming.py
```

Nếu Spark báo thiếu Kafka connector, chạy bằng `spark-submit` với package tương ứng Spark 3.5/Scala 2.12:

```powershell
\.venv\Scripts\spark-submit.cmd --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming\spark_streaming.py
```

## 4. Chạy producer

Mở PowerShell thứ hai:

```powershell
\.venv\Scripts\python.exe streaming\live_producer.py
```

Dữ liệu được lấy từ Open-Meteo theo giờ, không còn dùng CSV mẫu, và được ghi vào `data/streaming/air_quality` dưới dạng Parquet. Dừng Kafka bằng:

```powershell
docker compose down
```
