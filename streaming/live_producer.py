"""Tầng thu nạp dữ liệu (Ingestion) và bộ đệm hàng đợi phân tán Apache Kafka.

Module này thực hiện các nhiệm vụ:
1. Định kỳ gọi Open-Meteo REST API để thu thập các chỉ số khí tượng thời gian thực
   (PM2.5, PM10, CO, NO2, SO2, O3, European AQI) của 4 đô thị trọng điểm: TP.HCM, Hà Nội, Đà Nẵng, Cần Thơ.
2. Tuần tự hóa bản ghi dữ liệu thành định dạng JSON gọn nhẹ.
3. Đẩy luồng sự kiện vào Apache Kafka Topic 'air-quality' tại cổng 9092.
4. Đảm bảo tính khử ghép nối (Decoupling) và chống quá tải đường truyền (Backpressure)
   giữa tầng thu nạp và tầng xử lý luồng phân tán Apache Spark.
"""

import json
import os
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

# -----------------------------------------------------------------------------
# 1. CẤU HÌNH KAFKA CLUSTER VÀ NGUỒN DỮ LIỆU API
# -----------------------------------------------------------------------------
# Địa chỉ Kafka Broker phân tán (mặc định localhost:9092 từ cụm Docker Compose)
BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Tên Topic chuyên biệt trong Kafka lưu trữ luồng dữ liệu môi trường
TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")

# Endpoint REST API quan trắc chất lượng không khí toàn cầu Open-Meteo
API = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Tọa độ địa lý và mã định danh của 4 đô thị trọng điểm tại Việt Nam
LOCATIONS = [
    {"location_id": "hcm", "location_name": "TP.HCM", "latitude": 10.8231, "longitude": 106.6297},
    {"location_id": "hanoi", "location_name": "Hà Nội", "latitude": 21.0285, "longitude": 105.8542},
    {"location_id": "danang", "location_name": "Đà Nẵng", "latitude": 16.0544, "longitude": 108.2022},
    {"location_id": "cantho", "location_name": "Cần Thơ", "latitude": 10.0452, "longitude": 105.7469},
]


# -----------------------------------------------------------------------------
# 2. HÀM THU THẬP DỮ LIỆU TỪ REST API OPEN-METEO
# -----------------------------------------------------------------------------
def fetch_latest(location):
    """Gửi HTTP GET Request kéo số đo quan trắc mới nhất theo thời gian thực của trạm đo.

    - Truy vấn 7 thông số nồng độ chất gây ô nhiễm và chỉ số chất lượng không khí chuẩn Châu Âu.
    - Tìm mốc thời gian trong dữ liệu trả về có khoảng cách gần nhất với thời điểm hiện tại (UTC).
    - Đóng gói dữ liệu kèm mã định danh trạm và tọa độ địa lý.
    """
    response = requests.get(
        API,
        params={
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "hourly": "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,european_aqi",
            "forecast_days": 2,
            "timezone": "UTC",
        },
        timeout=30
    )
    response.raise_for_status()

    hourly = response.json()["hourly"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Lấy chỉ số của bản ghi có mốc thời gian sát với hiện tại nhất
    i = min(range(len(hourly["time"])), key=lambda n: abs(datetime.fromisoformat(hourly["time"][n]) - now))

    return {
        **location,
        "timestamp": hourly["time"][i],
        "pm25": hourly["pm2_5"][i],
        "pm10": hourly["pm10"][i],
        "co": hourly["carbon_monoxide"][i],
        "no2": hourly["nitrogen_dioxide"][i],
        "so2": hourly["sulphur_dioxide"][i],
        "o3": hourly["ozone"][i],
        "aqi": hourly["european_aqi"][i],
    }


# -----------------------------------------------------------------------------
# 3. VÒNG LẶP PHÁT STREAM VÀO KAFKA CLUSTER (EVENT STREAMING)
# -----------------------------------------------------------------------------
# Khởi tạo đối tượng KafkaProducer với cơ chế tuần tự hóa JSON sang UTF-8 bytes
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print(f"[*] Live Producer đã kết nối tới Kafka Broker: {BROKER}")
print(f"[*] Topic: '{TOPIC}' | Đang giám sát 4 đô thị: TP.HCM, Hà Nội, Đà Nẵng, Cần Thơ.\n")

try:
    while True:
        for location in LOCATIONS:
            record = fetch_latest(location)

            # Bắn thông điệp bất đồng bộ vào Kafka topic
            producer.send(TOPIC, value=record)
            print(f"[➔ Đã đẩy Kafka] {record['location_name']:<10} | PM2.5: {record['pm25']:<5} | AQI: {record['aqi']:<4} | Timestamp: {record['timestamp']}")

        # Đẩy toàn bộ dữ liệu từ hàng đợi RAM ra Kafka Broker
        producer.flush()

        # Nghỉ theo chu kỳ thu thập dữ liệu quan trắc
        print("\n[Vòng lặp hoàn tất] Tạm nghỉ trước đợt cập nhật tiếp theo...\n")
        time.sleep(3600)
finally:
    # Đóng kết nối producer an toàn khi tiến trình dừng
    producer.close()
    print("[*] Đã đóng kết nối Kafka Producer an toàn.")
