# Kế hoạch đồ án: Dự báo chất lượng không khí

## 1. Thông tin định hướng

- **Đề tài dự kiến:** Xây dựng hệ thống dự báo chất lượng không khí bằng công nghệ dữ liệu lớn.
- **Khu vực:** Nhiều địa điểm có trong dataset, mỗi địa điểm được xác định bằng tên trạm, thành phố hoặc tọa độ.
- **Biến cần dự báo:** AQI hoặc PM2.5 tại một thời điểm trong tương lai, trước mắt là 1 giờ tiếp theo.
- **Công nghệ dự kiến:** Python, Apache Spark, Spark MLlib, HDFS hoặc MinIO, Streamlit và MongoDB/PostgreSQL.
- **Sản phẩm cuối:** Một ứng dụng dashboard có thể hiển thị dữ liệu lịch sử và kết quả dự báo.

> Có thể điều chỉnh khu vực, biến dự báo và công nghệ sau khi khảo sát dataset thực tế.

---

## 2. Giai đoạn 1: Khảo sát đề tài

### 2.1. Chọn khu vực nghiên cứu

- [x] Chọn **một trạm quan trắc tại thành phố Rome, Ý** theo khu vực của UCI Air Quality Dataset.
- [x] Ghi rõ lý do chọn khu vực.
- [x] Xác định khoảng thời gian sử dụng dữ liệu: từ tháng 3/2004 đến tháng 2/2005, theo khoảng thời gian có trong dataset.
- [x] Xác định dự báo cho một trạm quan trắc.

#### Khu vực được chọn

Đề tài sẽ sử dụng **nhiều địa điểm/trạm quan trắc**. Người dùng chỉ được chọn trong danh sách các địa điểm có dữ liệu lịch sử. Mỗi bản ghi cần có tối thiểu một mã địa điểm và nên có tên thành phố hoặc tọa độ.

#### Trường hợp phần mềm cho phép chọn địa điểm

Nếu yêu cầu của phần mềm là người dùng có thể chọn **một địa điểm bất kỳ có trong dữ liệu**, dataset phải có ít nhất một cột xác định địa điểm, chẳng hạn:

```text
location_id
station_name
city
latitude
longitude
timestamp
pm25
pm10
aqi
```

Khi đó, luồng hoạt động sẽ là:

```text
Người dùng chọn địa điểm
        ↓
Lọc dữ liệu theo location_id hoặc station_name
        ↓
Nạp mô hình tương ứng hoặc mô hình dùng chung
        ↓
Dự báo AQI/PM2.5 cho địa điểm đã chọn
```

Dataset UCI hiện tại chỉ phù hợp với **một trạm**, vì vậy không thể đáp ứng đầy đủ chức năng chọn nhiều địa điểm. Nếu chức năng này là yêu cầu bắt buộc, cần đổi nguồn dữ liệu chính sang nguồn có nhiều trạm như OpenAQ hoặc một dataset nhiều thành phố/trạm trên Kaggle.

Khi dùng dữ liệu nhiều trạm, có hai cách xây dựng mô hình:

- **Mô hình dùng chung:** thêm `location_id`, tọa độ hoặc tên thành phố làm đặc trưng. Cách này đơn giản hơn khi có nhiều địa điểm.
- **Mỗi địa điểm một mô hình:** huấn luyện một mô hình riêng cho từng trạm. Cách này có thể chính xác hơn nhưng cần đủ dữ liệu cho từng trạm.

Đối với phiên bản đầu tiên, nên dùng **một mô hình dùng chung** và cho phép người dùng chọn trong danh sách các địa điểm thực sự tồn tại trong dữ liệu. Không nên cho nhập một địa điểm hoàn toàn tùy ý nếu hệ thống không có dữ liệu lịch sử cho địa điểm đó.

#### Lý do lựa chọn

- Dataset có dữ liệu đo theo thời gian và phù hợp để xây dựng bài toán dự báo.
- Nhiều địa điểm giúp phần mềm có chức năng chọn khu vực và thể hiện rõ chiều không gian của dữ liệu.
- Dữ liệu có nhiều chỉ số ô nhiễm và yếu tố môi trường.
- Mỗi địa điểm có đủ dữ liệu lịch sử để dự báo tại thời điểm tương lai.
- Hệ thống có thể dùng một mô hình chung hoặc mô hình riêng cho từng địa điểm.

#### Phạm vi khu vực

- **Phạm vi không gian:** Các địa điểm có trong dataset.
- **Phạm vi thời gian:** Khoảng thời gian lịch sử liên tục có trong dataset.
- **Số trạm:** Từ hai địa điểm trở lên nếu dữ liệu đáp ứng yêu cầu.
- **Dữ liệu đầu vào:** Các chỉ số chất lượng không khí và thời tiết được ghi nhận theo giờ hoặc ngày.
- **Thời điểm dự báo:** 1 giờ tiếp theo; có thể mở rộng thành 6 hoặc 24 giờ.

> `AirQualityUCI.csv` chỉ có một trạm nên không đủ cho yêu cầu nhiều địa điểm. File này chỉ nên giữ lại để kiểm tra pipeline ban đầu. Nguồn chính cần chuyển sang dataset nhiều trạm hoặc dữ liệu thu thập từ OpenAQ/Open-Meteo.

### 2.2. Tìm và chọn nguồn dữ liệu

- [x] Chọn dataset nền **UCI Air Quality Dataset** để làm phiên bản đầu tiên.
- [ ] Tìm dataset trên Kaggle hoặc OpenAQ để mở rộng sang Việt Nam nếu dữ liệu phù hợp.
- [ ] Tìm dữ liệu thời tiết nếu muốn bổ sung nhiệt độ, độ ẩm và tốc độ gió.
- [ ] Kiểm tra giấy phép sử dụng dữ liệu.
- [x] Ghi nhận nguồn dữ liệu và cách tải trong báo cáo.
- [ ] Tải dataset về thư mục `data/raw`.

#### Nguồn dữ liệu được chọn cho phiên bản đầu tiên

**Tên:** UCI Air Quality Dataset  
**Trang nguồn:** https://archive.ics.uci.edu/dataset/360/air+quality  
**Khu vực:** Một trạm quan trắc tại thành phố Ý (không phải TP.HCM).  
**Định dạng:** File dữ liệu dạng bảng, có dữ liệu đo theo giờ trong nhiều tháng.  
**Các biến chính:** CO, NMHC, benzen, NOx, NO2, nhiệt độ, độ ẩm tương đối và độ ẩm tuyệt đối.  
**Mục tiêu đề xuất:** Dự báo nồng độ CO hoặc NO2 ở giờ kế tiếp.  

Dataset này được chọn để nhóm có dữ liệu sạch ở mức đủ dùng và có thể tập trung học quy trình Big Data. Khi đã chạy được toàn bộ pipeline, nhóm có thể thay nguồn dữ liệu bằng dữ liệu tại TP.HCM mà không phải thay đổi toàn bộ kiến trúc.

#### Nguồn mở rộng nếu cần dữ liệu gần Việt Nam

1. **OpenAQ:** https://openaq.org/  
   Cung cấp dữ liệu chất lượng không khí từ các trạm quan trắc trên thế giới thông qua API. Cần kiểm tra xem thời điểm thực hiện có trạm và dữ liệu lịch sử phù hợp tại TP.HCM hay không; một số phiên bản API có thể yêu cầu API key.
2. **Open-Meteo Air Quality API:** https://open-meteo.com/en/docs/air-quality-api  
   Cung cấp dữ liệu chất lượng không khí theo tọa độ, trong đó có PM2.5, PM10, CO, NO2, SO2 và O3. Có thể dùng để lấy dữ liệu cho tọa độ TP.HCM và kết hợp với dữ liệu thời tiết.
3. **Kaggle:** https://www.kaggle.com/datasets  
   Có nhiều dataset sẵn ở dạng CSV, thuận tiện để thử nghiệm. Khi chọn trên Kaggle, cần kiểm tra kỹ thời gian, khu vực, số dòng, dữ liệu thiếu và giấy phép.

#### Quyết định sử dụng dữ liệu

- **Bản tối thiểu để hoàn thành:** UCI Air Quality Dataset.
- **Bản có tính địa phương hơn:** OpenAQ hoặc Open-Meteo với tọa độ TP.HCM, nếu dữ liệu có đủ bản ghi liên tục.
- **Không nên trộn dữ liệu của hai khu vực khác nhau** vào cùng một mô hình nếu chưa xử lý biến `location` và chưa giải thích rõ trong báo cáo.

#### Cách tải và lưu dữ liệu

1. Mở trang UCI ở trên.
2. Tải file dataset về máy.
3. Giải nén nếu file ở dạng `.zip`.
4. Đổi tên file thành `uci_air_quality.csv` nếu cần.
5. Lưu vào thư mục `data/raw` của dự án.
6. Ghi lại ngày tải và đường dẫn nguồn trong báo cáo.

> Lưu ý: Dataset UCI có thể dùng dấu phân cách khác dấu phẩy và có giá trị thiếu được biểu diễn bằng ký hiệu đặc biệt. Khi sang giai đoạn xử lý dữ liệu, cần kiểm tra delimiter, encoding và ký hiệu giá trị thiếu trước khi đọc bằng pandas hoặc Spark.

### 2.3. Khảo sát dataset

- [ ] Ghi tên và nguồn dataset.
- [ ] Kiểm tra số lượng bản ghi.
- [ ] Kiểm tra khoảng thời gian của dữ liệu.
- [ ] Kiểm tra tần suất đo: phút, giờ hoặc ngày.
- [ ] Liệt kê các cột dữ liệu.
- [ ] Kiểm tra dữ liệu thiếu.
- [ ] Kiểm tra dữ liệu trùng lặp.
- [ ] Kiểm tra dữ liệu bất thường hoặc sai định dạng.
- [ ] Xác định dataset có cột AQI hay chỉ có PM2.5/PM10.

### 2.4. Xác định bài toán

- [ ] Chọn dự báo giá trị AQI/PM2.5 hoặc phân loại mức độ ô nhiễm.
- [ ] Xác định thời điểm cần dự báo: 1 giờ, 6 giờ hoặc 24 giờ tiếp theo.
- [ ] Xác định biến đầu vào.
- [ ] Xác định biến đầu ra.
- [ ] Viết câu hỏi nghiên cứu.
- [ ] Viết mục tiêu tổng quát và các mục tiêu cụ thể.

### 2.5. Đọc tài liệu liên quan

- [ ] Tìm ít nhất 5 bài báo, đồ án hoặc tài liệu liên quan.
- [ ] Ghi lại nguồn dữ liệu của từng tài liệu.
- [ ] Ghi lại mô hình được sử dụng.
- [ ] Ghi lại các chỉ số đánh giá.
- [ ] Ghi lại kết quả và hạn chế của từng tài liệu.
- [ ] Lập bảng so sánh các tài liệu.

### 2.6. Vẽ kiến trúc hệ thống

- [x] Vẽ nguồn dữ liệu.
- [x] Vẽ lớp thu thập dữ liệu.
- [x] Vẽ nơi lưu trữ dữ liệu thô.
- [x] Vẽ bước xử lý bằng Apache Spark.
- [x] Vẽ bước huấn luyện mô hình.
- [x] Vẽ nơi lưu kết quả dự báo.
- [x] Vẽ dashboard hiển thị kết quả.
- [x] Viết mô tả ngắn cho từng thành phần.

#### Kiến trúc hệ thống phiên bản hiện tại

```text
+--------------------------+
| AirQualityUCI.csv        |
| Dữ liệu lịch sử theo giờ |
+------------+-------------+
             |
             v
+--------------------------+
| Lớp thu thập dữ liệu     |
| load_data() / Python     |
+------------+-------------+
             |
             v
+--------------------------+
| Lưu trữ dữ liệu thô      |
| air+quality/             |
| AirQualityUCI.csv        |
+------------+-------------+
             |
             v
+--------------------------+
| Xử lý dữ liệu            |
| PySpark / Pandas         |
| - Đọc dấu ;              |
| - Đổi dấu thập phân      |
| - Xử lý -200             |
| - Chuẩn hóa thời gian    |
| - Tạo biến trễ           |
+------------+-------------+
             |
             v
+--------------------------+
| Huấn luyện mô hình       |
| RandomForestRegressor    |
| Dự báo CO ở giờ kế tiếp  |
+------------+-------------+
             |
             v
+--------------------------+
| Lưu model và kết quả     |
| models/                  |
| air_quality_model.joblib |
+------------+-------------+
             |
             v
+--------------------------+
| Streamlit Dashboard      |
| app.py                   |
| Biểu đồ và dự báo        |
+--------------------------+
```

#### Mô tả các thành phần

**1. Nguồn dữ liệu**  
Hệ thống sử dụng file `AirQualityUCI.csv`, chứa dữ liệu chất lượng không khí được ghi nhận theo giờ tại một trạm quan trắc ở Rome, Ý trong khoảng thời gian từ ngày 10/03/2004 đến ngày 04/04/2005.

**2. Lớp thu thập dữ liệu**  
Ở phiên bản hiện tại, chương trình đọc file CSV từ thư mục dự án bằng hàm `load_data()` trong `src/air_quality.py`. Trong phiên bản mở rộng, lớp này có thể gọi API OpenAQ hoặc Open-Meteo để lấy dữ liệu mới.

**3. Lưu trữ dữ liệu thô**  
Dữ liệu gốc được giữ nguyên trong thư mục `air+quality/`. Việc giữ dữ liệu gốc giúp có thể xử lý lại từ đầu khi thay đổi cách làm sạch hoặc tạo đặc trưng.

**4. Xử lý dữ liệu**  
Chương trình chuẩn hóa dấu phân cách, dấu thập phân, giá trị thiếu `-200` và cột thời gian. Sau đó tạo các đặc trưng như CO của giờ trước, CO của 3 giờ trước và giờ trong ngày. File `process_with_spark.py` minh họa bước xử lý bằng Apache Spark và xuất dữ liệu dạng Parquet.

**5. Huấn luyện mô hình**  
Mô hình `RandomForestRegressor` học mối quan hệ giữa các chỉ số môi trường và nồng độ CO hiện tại để dự báo nồng độ CO ở giờ kế tiếp. Dữ liệu được chia theo thứ tự thời gian: 80% đầu dùng để huấn luyện và 20% cuối dùng để kiểm thử.

**6. Lưu model và kết quả**  
Model được lưu tại `models/air_quality_model.joblib`. File này chứa mô hình, danh sách đặc trưng và các chỉ số đánh giá như MAE, RMSE và R².

**7. Dashboard**  
File `app.py` sử dụng Streamlit để hiển thị nồng độ CO hiện tại, giá trị dự báo ở giờ kế tiếp, biểu đồ dữ liệu lịch sử và các chỉ số đánh giá mô hình.

#### Mở rộng thành hệ thống dữ liệu mới

Khi tích hợp API, kiến trúc có thể mở rộng như sau:

```text
OpenAQ / Open-Meteo API
          |
          v
  Python API Collector
          |
          v
   HDFS / MinIO / Kafka
          |
          v
  Spark / Spark Streaming
          |
          v
  Mô hình dự báo đã lưu
          |
          v
       Dashboard
```

Kiến trúc mở rộng này cho phép hệ thống nhận dữ liệu mới định kỳ hoặc gần thời gian thực. Đây là hướng phát triển, không bắt buộc phải hoàn thành trong phiên bản đầu tiên.

**Kiến trúc dự kiến:**

```text
Kaggle / OpenAQ / Weather API
            ↓
    Thu thập dữ liệu
            ↓
     Lưu dữ liệu thô
     HDFS hoặc MinIO
            ↓
    Xử lý bằng Spark
            ↓
      Spark MLlib
   Huấn luyện mô hình
            ↓
    Lưu kết quả dự báo
            ↓
       Streamlit
        Dashboard
```

**Kết quả cần đạt:** Tên đề tài, khu vực, dataset, biến dự báo, mục tiêu, tài liệu tham khảo và sơ đồ kiến trúc.

---

## 3. Giai đoạn 2: Chuẩn bị môi trường

- [ ] Cài Python.
- [ ] Cài Java phù hợp với Apache Spark.
- [ ] Cài Apache Spark và PySpark.
- [ ] Cài Jupyter Notebook hoặc Visual Studio Code.
- [ ] Cài thư viện xử lý dữ liệu: pandas, numpy.
- [ ] Cài thư viện trực quan hóa: matplotlib, plotly.
- [ ] Cài Streamlit.
- [ ] Kiểm tra chương trình PySpark đầu tiên.
- [ ] Tạo file `requirements.txt`.

---

## 4. Giai đoạn 3: Thu thập và xử lý dữ liệu

- [ ] Lưu dữ liệu gốc vào thư mục `data/raw`.
- [ ] Viết chương trình đọc dữ liệu.
- [ ] Chuẩn hóa tên các cột.
- [ ] Chuẩn hóa định dạng thời gian.
- [ ] Loại bỏ bản ghi trùng.
- [ ] Xử lý giá trị thiếu.
- [ ] Xử lý giá trị bất thường.
- [ ] Sắp xếp dữ liệu theo thời gian.
- [ ] Tổng hợp dữ liệu theo giờ nếu cần.
- [ ] Tạo các đặc trưng thời gian: giờ, ngày, tháng, thứ trong tuần.
- [ ] Tạo biến trễ: AQI/PM2.5 của giờ trước.
- [ ] Tạo biến mục tiêu: AQI/PM2.5 của giờ kế tiếp.
- [ ] Lưu dữ liệu đã xử lý vào `data/processed`.
- [ ] Thực hiện các bước chính bằng Spark.

---

## 5. Giai đoạn 4: Xây dựng mô hình dự báo

- [ ] Chia dữ liệu theo thứ tự thời gian thành train, validation và test.
- [ ] Xây dựng phương pháp cơ sở: dùng giá trị hiện tại để dự báo giờ tiếp theo.
- [ ] Huấn luyện mô hình Linear Regression.
- [ ] Huấn luyện mô hình Random Forest.
- [ ] Có thể thử Gradient Boosting nếu còn thời gian.
- [ ] Lưu mô hình tốt nhất.
- [ ] Viết chương trình nạp mô hình và dự báo dữ liệu mới.

### Chỉ số đánh giá nếu dự báo giá trị

- [ ] MAE.
- [ ] RMSE.
- [ ] MAPE nếu phù hợp.
- [ ] R².

### Nếu phân loại mức độ ô nhiễm

- [ ] Accuracy.
- [ ] Precision.
- [ ] Recall.
- [ ] F1-score.
- [ ] Confusion matrix.

---

## 6. Giai đoạn 5: Xây dựng phần mềm thử nghiệm

- [ ] Tạo giao diện bằng Streamlit.
- [ ] Hiển thị thông tin khu vực/trạm đo.
- [ ] Hiển thị AQI hoặc PM2.5 hiện tại.
- [ ] Hiển thị biểu đồ dữ liệu lịch sử.
- [ ] Cho phép chọn khoảng thời gian.
- [ ] Cho phép chọn trạm nếu có nhiều trạm.
- [ ] Hiển thị kết quả dự báo.
- [ ] Hiển thị mức cảnh báo bằng màu sắc.
- [ ] Hiển thị biểu đồ giá trị thực tế và giá trị dự báo.
- [ ] Hiển thị các chỉ số đánh giá mô hình.
- [ ] Kiểm tra giao diện trên máy khác nếu có thể.

### Chức năng tối thiểu

```text
Đọc dữ liệu → Chọn khu vực → Dự báo → Hiển thị kết quả
```

### Chức năng mở rộng nếu còn thời gian

- [ ] Tự động cập nhật dữ liệu từ API.
- [ ] Bổ sung bản đồ các trạm quan trắc.
- [ ] Bổ sung cảnh báo khi AQI vượt ngưỡng.
- [ ] Bổ sung Kafka và Spark Streaming.
- [ ] Triển khai ứng dụng trên máy chủ hoặc nền tảng đám mây.

---

## 7. Giai đoạn 6: Kiểm thử

- [ ] Kiểm tra dữ liệu đầu vào không bị lỗi.
- [ ] Kiểm tra chương trình xử lý được dữ liệu thiếu.
- [ ] Kiểm tra mô hình có thể nạp lại sau khi tắt chương trình.
- [ ] Kiểm tra kết quả dự báo có đúng định dạng.
- [ ] Kiểm tra dashboard khi không có dữ liệu.
- [ ] Kiểm tra trường hợp người dùng chọn sai khoảng thời gian.
- [ ] Ghi lại lỗi và cách khắc phục.

---

## 8. Giai đoạn 7: Viết báo cáo

### Chương 1: Tổng quan

- [ ] Lý do chọn đề tài.
- [ ] Mục tiêu đề tài.
- [ ] Phạm vi đề tài.
- [ ] Đối tượng nghiên cứu.

### Chương 2: Cơ sở lý thuyết

- [ ] Khái niệm Big Data và 5V.
- [ ] Tổng quan về chất lượng không khí và AQI.
- [ ] Apache Spark.
- [ ] Spark SQL và Spark MLlib.
- [ ] Mô hình machine learning được chọn.

### Chương 3: Phân tích và thiết kế

- [ ] Mô tả nguồn dữ liệu.
- [ ] Mô tả các cột dữ liệu.
- [ ] Quy trình xử lý dữ liệu.
- [ ] Kiến trúc hệ thống.
- [ ] Thiết kế cơ sở dữ liệu nếu có.
- [ ] Thiết kế giao diện.

### Chương 4: Cài đặt và thực nghiệm

- [ ] Môi trường cài đặt.
- [ ] Các bước xử lý dữ liệu.
- [ ] Huấn luyện mô hình.
- [ ] Kết quả đánh giá.
- [ ] Hình ảnh phần mềm.
- [ ] Một số trường hợp chạy thử.

### Chương 5: Kết luận và hướng phát triển

- [ ] Những kết quả đạt được.
- [ ] Những hạn chế.
- [ ] Hướng mở rộng: dữ liệu thời gian thực, Kafka, nhiều trạm, LSTM.

---

## 9. Phân công công việc gợi ý

| Công việc | Người phụ trách | Trạng thái | Ghi chú |
|---|---|---|---|
| Tìm dataset |  | Chưa làm |  |
| Khảo sát dữ liệu |  | Chưa làm |  |
| Đọc tài liệu liên quan |  | Chưa làm |  |
| Xử lý dữ liệu |  | Chưa làm |  |
| Xây dựng mô hình |  | Chưa làm |  |
| Xây dựng dashboard |  | Chưa làm |  |
| Kiểm thử |  | Chưa làm |  |
| Viết báo cáo |  | Chưa làm |  |
| Chuẩn bị thuyết trình |  | Chưa làm |  |

---

## 10. Thứ tự ưu tiên khi thực hiện

Nếu thời gian hạn chế, hãy ưu tiên theo thứ tự:

1. Chọn được dataset phù hợp.
2. Xác định rõ biến cần dự báo.
3. Xử lý dữ liệu thành công.
4. Huấn luyện được một mô hình.
5. Đánh giá được mô hình.
6. Làm dashboard tối thiểu.
7. Tích hợp Spark vào pipeline.
8. Bổ sung Kafka hoặc dự báo thời gian thực.

Không nên bắt đầu bằng Kafka, LSTM hoặc giao diện trước khi có dữ liệu sạch và mô hình chạy được.
