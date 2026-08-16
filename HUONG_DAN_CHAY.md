# Huong dan chay he thong du bao chat luong khong khi realtime

He thong chay theo luong:

```text
Open-Meteo API -> Kafka -> Spark Structured Streaming -> Parquet -> 24 model MLlib -> Parquet du bao -> Streamlit
```

## Dieu kien can co

- Docker Desktop dang chay.
- Python virtual environment tai `.venv`.
- Da cai dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Chay he thong

### Cach nhanh: mot lenh

Mo PowerShell tai thu muc du an va chay:

```powershell
.\start-all.ps1
```

Script se tu dong khoi dong Kafka, tao topic `air-quality` neu can, mo Spark ingestion, producer, prediction stream va dashboard trong cac cua so rieng.

Neu PowerShell chan script:

```powershell
powershell -ExecutionPolicy Bypass -File .\start-all.ps1
```

Sau do mo dashboard tai `http://localhost:8501`.

### Cach thu cong

Mo 5 cua so PowerShell tai thu muc du an:

```powershell
cd D:\IT\BigData\BigDataProject
```

### Terminal 1: Kafka

```powershell
docker compose up -d
docker ps
```

Container `air-quality-kafka` phai co trang thai `Up`.

Neu topic chua ton tai, tao topic mot lan:

```powershell
docker exec air-quality-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic air-quality --partitions 1 --replication-factor 1
```

### Terminal 2: Spark Structured Streaming

```powershell
.\.venv\Scripts\spark-submit.cmd --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming\spark_streaming.py
```

Spark se chay lien tuc va khong tra lai dau nhac PowerShell. Giu terminal nay mo.

### Terminal 3: Producer du lieu moi

```powershell
.\.venv\Scripts\python.exe streaming\live_producer.py
```

Producer lay du lieu Open-Meteo cho TP.HCM, Ha Noi, Da Nang va Can Tho, sau do gui vao Kafka. Lan gui dau tien xuat hien ngay; cac lan sau cach nhau mot gio.

### Terminal 4: Du bao PM2.5 bang Spark MLlib

Chi can huan luyen 24 model mot lan, hoac huan luyen lai khi ban thay doi du lieu/model:

```powershell
.\.venv\Scripts\python.exe train_multi_horizon_models.py
```

Sau khi thu muc `models\pm25_multi_horizon_spark\h1` den `h24` da ton tai, cac lan chay sau khong can train lai. Chi chay prediction stream:

```powershell
.\.venv\Scripts\python.exe streaming\spark_multi_horizon_prediction_stream.py
```

Tien trinh nay doc Parquet realtime, tao cac bien tre `lag1`, `lag3`, trung binh 3 gio, va luu du bao truc tiep cho moi moc 1-24 gio vao `data\streaming\multi_horizon_predictions`. Du bao dau tien cua moi tram chi xuat hien sau khi da co it nhat 3 ban ghi theo gio.

Neu doi model hoac sua logic timezone, dung prediction stream cu bang `Ctrl + C`, doi ten checkpoint/du lieu du bao cu sang thu muc backup, sau do chay lai prediction stream.

### Terminal 5: Dashboard

```powershell
.\.venv\Scripts\python.exe -m streamlit run app.py
```

Mo trinh duyet tai:

```text
http://localhost:8501
```

Dashboard doc du lieu Parquet tai `data\streaming\air_quality` va tu lam moi theo chu ky da chon.

## Kiem tra pipeline

Sau khi producer in du lieu, kiem tra Spark da ghi Parquet:

```powershell
Get-ChildItem data\streaming\air_quality -Recurse
```

Can co cac file co duoi `.parquet`.

Kiem tra ket qua du bao:

```powershell
Get-ChildItem data\streaming\multi_horizon_predictions -Recurse
```

Dashboard chi hien gia tri cho moc du bao da duoc tao. Neu chon 24 gio khi chua co model `h24` hoac chua co du lieu lich su, KPI se hien `Dang cho du lieu`.

## Dung he thong

Dung Streamlit, producer va Spark bang cach nhan truc tiep `Ctrl + C` tai tung terminal dang chay.

Dung Kafka:

```powershell
docker compose down
```

Khong dung `docker compose down -v`, vi lenh nay xoa Docker volume chua du lieu Kafka.

## Loi thuong gap

| Loi | Cach xu ly |
| --- | --- |
| `docker is not recognized` | Mo Docker Desktop, sau do mo PowerShell moi. |
| `UnknownTopicOrPartitionException` | Tao topic `air-quality` bang lenh o phan Kafka. |
| `400 Bad Request` tu Open-Meteo | Cap nhat `streaming/live_producer.py`, sau do chay lai producer. |
| Dashboard bao chua co streaming data | Kiem tra Spark va producer dang chay, sau do kiem tra file Parquet. |
| KPI hien `Dang cho du lieu` | Kiem tra prediction stream dang chay, da train model h1-h24 va da chon dung moc du bao. |
| `Path does not exist` khi chay prediction stream | Chay Spark Structured Streaming truoc de tao `data\\streaming\\air_quality`. |
| Docker bao thieu virtualization | Bat virtualization trong BIOS va cai WSL 2. |
