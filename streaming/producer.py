"""Gửi từng bản ghi chất lượng không khí vào Kafka để mô phỏng realtime."""
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "multi_location_air_quality.csv")


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    data = pd.read_csv(DATA_FILE).sort_values("timestamp")
    print(f"Producing {len(data)} records to {TOPIC} via {BROKER}")
    try:
        for record in data.to_dict(orient="records"):
            record["timestamp"] = datetime.now(timezone.utc).isoformat()
            producer.send(TOPIC, value=record)
            producer.flush()
            print(record)
            time.sleep(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
