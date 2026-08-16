"""Download hourly multi-location air-quality data from Open-Meteo."""

import csv
import json
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "data" / "raw" / "multi_location_air_quality.csv"
START = date(2023, 1, 1)
END = date(2025, 12, 31)
LOCATIONS = {
    "hcm": ("TP.HCM", 10.8231, 106.6297),
    "hanoi": ("Ha Noi", 21.0278, 105.8342),
    "danang": ("Da Nang", 16.0544, 108.2022),
    "cantho": ("Can Tho", 10.0452, 105.7469),
}
HOURLY = (
    "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,"
    "sulphur_dioxide,ozone,european_aqi"
)


def fetch(name, latitude, longitude):
    params = urlencode({
        "latitude": latitude,
        "longitude": longitude,
        "hourly": HOURLY,
        "start_date": START.isoformat(),
        "end_date": END.isoformat(),
        "timezone": "Asia/Bangkok",
    })
    with urlopen("https://air-quality-api.open-meteo.com/v1/air-quality?" + params, timeout=60) as response:
        payload = json.load(response)
    hourly = payload["hourly"]
    rows = []
    for i, timestamp in enumerate(hourly["time"]):
        rows.append({
            "location_id": name,
            "location_name": LOCATIONS[name][0],
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": timestamp,
            "pm25": hourly["pm2_5"][i],
            "pm10": hourly["pm10"][i],
            "co": hourly["carbon_monoxide"][i],
            "no2": hourly["nitrogen_dioxide"][i],
            "so2": hourly["sulphur_dioxide"][i],
            "o3": hourly["ozone"][i],
            "aqi": hourly["european_aqi"][i],
        })
    return rows


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for location_id, values in LOCATIONS.items():
        print(f"Downloading {values[0]}...")
        rows.extend(fetch(location_id, values[1], values[2]))
        time.sleep(1)
    fields = list(rows[0])
    with OUTPUT.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows):,} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
