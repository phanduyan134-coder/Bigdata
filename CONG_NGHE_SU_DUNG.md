# Công nghệ sử dụng trong project

## 1. Tổng quan

Project xây dựng dashboard dự báo chất lượng không khí theo thời gian thực cho
bốn khu vực: Hà Nội, Đà Nẵng, TP.HCM và Cần Thơ.

Luồng xử lý chính:

```text
Open-Meteo API
    -> live_producer.py
    -> Apache Kafka
    -> Spark Structured Streaming
    -> Parquet
    -> 24 mô hình dự báo PM2.5
    -> Streamlit dashboard
```

## 2. Nguồn dữ liệu

### Open-Meteo Air Quality API

Open-Meteo cung cấp dữ liệu chất lượng không khí theo giờ, gồm PM2.5, PM10 và
European AQI. Script `streaming/live_producer.py` gọi API, lấy bản ghi mới nhất
cho từng địa điểm và gửi chúng vào Kafka.

API giúp project nhận dữ liệu mới mà không cần tiếp tục sử dụng CSV mẫu cho
luồng realtime. CSV lịch sử vẫn được dùng để huấn luyện mô hình ban đầu.

## 3. Apache Kafka

Apache Kafka là nền tảng truyền tải dữ liệu dạng sự kiện. Trong project, Kafka
đóng vai trò message broker:

- Producer gửi dữ liệu quan trắc vào topic `air-quality`.
- Spark Streaming đọc dữ liệu từ topic này.
- Producer và Spark có thể chạy độc lập, không cần chờ nhau trực tiếp.

Kafka được chạy local bằng Docker Compose tại `localhost:9092`.

## 4. Docker

Docker đóng gói và chạy Kafka trong container `air-quality-kafka`. Docker giúp
môi trường Kafka nhất quán, dễ khởi động và không cần cài Kafka trực tiếp vào
Windows.

File cấu hình: `docker-compose.yml`.

## 5. Apache Spark

### Spark Structured Streaming

Spark Structured Streaming xử lý dữ liệu Kafka theo dạng streaming. Script
`streaming/spark_streaming.py` thực hiện các công việc chính:

- Đọc message JSON từ Kafka.
- Chuyển đổi kiểu dữ liệu và chuẩn hóa schema.
- Ghi dữ liệu thành các file Parquet trong `data/streaming/air_quality`.

### Spark MLlib

Spark MLlib được dùng để huấn luyện và chạy các mô hình dự báo PM2.5. Script
`train_multi_horizon_models.py` tạo 24 mô hình trực tiếp:

- Model `h1`: dự báo sau 1 giờ.
- Model `h2`: dự báo sau 2 giờ.
- ...
- Model `h24`: dự báo sau 24 giờ.

Việc dùng 24 mô hình giúp người dùng chọn trực tiếp khoảng thời gian muốn dự
báo. Khi có dữ liệu realtime mới, hệ thống dùng model đã huấn luyện để suy luận;
không cần train lại sau mỗi bản ghi.

Script `streaming/spark_multi_horizon_prediction_stream.py` đọc dữ liệu đã xử
lý, chạy 24 model và ghi kết quả vào
`data/streaming/multi_horizon_predictions`.

## 6. Machine Learning

### Random Forest Regression

Random Forest Regression dự báo giá trị PM2.5. Mô hình sử dụng các đặc trưng
theo thời gian, trong đó có các giá trị trễ của PM2.5 và thông tin thời điểm.

Mô hình được chọn vì có khả năng xử lý quan hệ phi tuyến và phù hợp với bài toán
dự báo dữ liệu môi trường. Các model Spark được lưu trong thư mục
`models/pm25_multi_horizon_spark`.

## 7. Định dạng lưu trữ Parquet

Parquet là định dạng cột, phù hợp cho dữ liệu phân tích lớn. Project dùng
Parquet để lưu dữ liệu streaming và kết quả dự báo vì:

- Kích thước thường nhỏ hơn CSV.
- Đọc nhanh các cột cần thiết.
- Tương thích tốt với Apache Spark.

## 8. Streamlit và Plotly

### Streamlit

Streamlit tạo dashboard web bằng Python tại `app.py`. Giao diện hiện có:

- Trang chủ hiển thị bản đồ Việt Nam.
- Bốn khu vực được biểu diễn theo chất lượng không khí.
- Nhấn vào khu vực để mở trang chi tiết.
- Chọn dự báo từ 1 đến 24 giờ.
- Hiển thị PM2.5, PM10, European AQI và biểu đồ dự báo.

### Plotly

Plotly tạo bản đồ tương tác và biểu đồ dữ liệu. GeoJSON trong
`data/geo/vietnam_adm1.geojson` được dùng để hiển thị ranh giới các tỉnh thành.

## 9. Thư viện Python chính

| Thư viện | Vai trò |
| --- | --- |
| `pyspark` | Xử lý streaming và Spark MLlib |
| `kafka-python` | Gửi dữ liệu vào Kafka |
| `requests` | Gọi Open-Meteo API |
| `pandas` | Đọc và chuẩn bị dữ liệu lịch sử |
| `scikit-learn` | Các pipeline/model ML bổ trợ |
| `joblib` | Lưu một số model Python |
| `streamlit` | Xây dựng dashboard web |
| `plotly` | Bản đồ và biểu đồ tương tác |

## 10. Vai trò trong kiến trúc Big Data

Project thể hiện các thành phần quan trọng của một hệ thống Big Data:

- **Data source:** Open-Meteo API và dữ liệu lịch sử.
- **Data ingestion:** `live_producer.py` và Kafka.
- **Stream processing:** Spark Structured Streaming.
- **Data storage:** Parquet.
- **Machine learning:** Spark MLlib và Random Forest Regression.
- **Data visualization:** Streamlit và Plotly.
- **Containerization:** Docker.

## 11. Giới hạn hiện tại

Đây là hệ thống streaming chạy local phục vụ học tập và demo. Kafka, Spark,
Parquet và dashboard hiện chạy trên cùng máy; dữ liệu đầu ra chưa được triển
khai lên cloud hoặc cụm Spark nhiều node. Vì vậy hệ thống chưa phải một nền tảng
sản xuất quy mô lớn, nhưng đã mô phỏng đầy đủ quy trình thu thập, truyền tải,
xử lý, dự báo và trực quan hóa dữ liệu thời gian thực.

