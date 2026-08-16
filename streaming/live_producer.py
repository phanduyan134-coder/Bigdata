"""Lấy dữ liệu Open-Meteo theo giờ và gửi trực tiếp vào Kafka."""
import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")
API = "https://air-quality-api.open-meteo.com/v1/air-quality"
LOCATIONS = [
    {"location_id": "hcm", "location_name": "TP.HCM", "latitude": 10.8231, "longitude": 106.6297},
    {"location_id": "hanoi", "location_name": "Hà Nội", "latitude": 21.0285, "longitude": 105.8542},
    {"location_id": "danang", "location_name": "Đà Nẵng", "latitude": 16.0544, "longitude": 108.2022},
    {"location_id": "cantho", "location_name": "Cần Thơ", "latitude": 10.0452, "longitude": 105.7469},
]


def fetch_latest(location):
    response = requests.get(API, params={
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,european_aqi",
        "forecast_days": 2,
        "timezone": "UTC",
    }, timeout=30)
    response.raise_for_status()
    hourly = response.json()["hourly"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    i = min(range(len(hourly["time"])), key=lambda n: abs(datetime.fromisoformat(hourly["time"][n]) - now))
    return {
        **location,
        "timestamp": hourly["time"][i],
        "pm25": hourly["pm2_5"][i], "pm10": hourly["pm10"][i],
        "co": hourly["carbon_monoxide"][i], "no2": hourly["nitrogen_dioxide"][i],
        "so2": hourly["sulphur_dioxide"][i], "o3": hourly["ozone"][i],
        "aqi": hourly["european_aqi"][i],
    }


producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda value: json.dumps(value).encode())
try:
    while True:
        for location in LOCATIONS:
            record = fetch_latest(location)
            producer.send(TOPIC, value=record)
            print(record)
        producer.flush()
        time.sleep(3600)
finally:
    producer.close()
