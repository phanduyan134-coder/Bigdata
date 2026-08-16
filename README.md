# Dự báo chất lượng không khí

## Mục tiêu

Đọc dữ liệu chất lượng không khí nhiều địa điểm từ Open-Meteo, xử lý dữ liệu theo thời gian, huấn luyện Random Forest để dự báo PM2.5 ở giờ kế tiếp và hiển thị kết quả trên dashboard Streamlit.

## Cài đặt

```powershell
py -m pip install -r requirements.txt
```

## Huấn luyện mô hình

```powershell
py train_model.py
```

Mô hình được lưu tại `models/pm25_multi_location_model.joblib`.

## Chạy phần mềm

```powershell
py -m streamlit run app.py
```

## Ghi chú

Dataset chính nằm tại `data/raw/multi_location_air_quality.csv` và có dữ liệu của TP.HCM, Hà Nội, Đà Nẵng và Cần Thơ từ năm 2023 đến 2025. Chương trình dự báo PM2.5 ở giờ kế tiếp bằng một mô hình dùng chung cho nhiều địa điểm. Phiên bản hiện tại dùng pandas để chạy dễ dàng; bước mở rộng của đồ án là chuyển các bước đọc, làm sạch và tổng hợp sang PySpark/HDFS hoặc MinIO.
