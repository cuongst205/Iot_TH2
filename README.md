# IoT TH2 - Python (không cần Docker)

Chỉ gồm các file Python và cấu hình để làm bài IoT TH2. Wokwi `sketch.ino`/`diagram.json` KHÔNG nằm trong ZIP này.

## 1. Chuẩn bị Windows

- Mở `influxd.exe` của InfluxDB 2.x và **giữ cửa sổ đó chạy**; kiểm tra `http://localhost:8086`.
- Organization: `PTIT` (nếu bạn đã đặt khác, sửa trong `.env`).
- Bucket dữ liệu gốc: `iot_th2`; bucket dữ liệu xử lý: `iot_processed`.
- Tạo Custom API Token đọc/ghi 2 bucket. **Không đăng token hoặc file .env lên mạng.**
- Máy trường đóng băng: sao lưu code + thư mục dữ liệu InfluxDB trước khi khởi động lại.

## 2. Cài thư viện

Mở CMD trong thư mục này:

```cmd
python -m pip install -r requirements.txt
```

Sao chép `.env.example` thành `.env` (trong CMD: `copy .env.example .env`), điền `INFLUX_TOKEN` bí mật của bạn.

## 3. Thử InfluxDB trước (KHÔNG CẦN MQTT)

```cmd
python test_influx.py
```

Vào Data Explorer, chọn bucket `iot_th2`, measurement `sensor_data`, device `esp32_test`, trường `temperature`, thời gian `Past 1h`.

## 4. Nhận dữ liệu ESP32 qua MQTT

Điền `MQTT_HOST`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `MQTT_TOPIC` trong `.env` theo broker Cloud đã tạo. Cấu hình ESP32 Wokwi gửi tới **cùng broker/topic**. Topic ThingsBoard `v1/devices/me/telemetry` không hoạt động như broker MQTT pub/sub thông thường đối với Python subscriber không có quyền subscribe; cần chuyển thiết bị sang broker MQTT chung.

Giữ CMD này chạy:

```cmd
python mqtt_to_influx.py
```

JSON ESP32 cũ gồm temperature, humidity, distance_cm, rssi, sequence, uptime_s là hợp lệ. Script sẽ bổ sung device_id mặc định `esp32_01` và timestamp theo thời điểm máy Python nhận. Chưa thể đo độ trễ end-to-end chính xác chỉ từ `uptime_s` vì nó không phải thời gian tuyệt đối; để đo đúng cần thêm timestamp đồng bộ NTP ở Wokwi.

## 5. Tiền xử lý

Sau khi ghi đủ dữ liệu (ví dụ vài phút), mở CMD khác:

```cmd
python preprocess.py --hours 24
```

Script: resampling 30 giây, làm sạch IQR, nội suy dữ liệu thiếu, rolling mean (3 cửa sổ), delta, chuẩn hóa min-max. Kết quả lưu measurement `sensor_processed` vào bucket `iot_processed`. Dữ liệu xử lý có thể cần hàng chục điểm mới giúp đánh giá outlier có ý nghĩa.

## 6. Dashboard

```cmd
python -m streamlit run dashboard.py
```

Trình duyệt mở tại `http://localhost:8501` (hoặc địa chỉ Streamlit báo trên CMD). Dashboard tự cập nhật 5 giây.

## Danh sách file

- `config.py`: đọc cấu hình từ `.env`
- `test_influx.py`: ghi/đọc thử dữ liệu InfluxDB
- `mqtt_to_influx.py`: subscribe MQTT, kiểm tra JSON và ghi InfluxDB
- `preprocess.py`: xử lý dữ liệu và lưu vào bucket xử lý
- `dashboard.py`: giao diện Streamlit
- `requirements.txt`, `.env.example`, `.gitignore`: hỗ trợ chạy project

## Giới hạn hiện tại

- Chưa kết nối Wokwi vào broker mới trong ZIP này vì bạn chưa tạo broker/host; sau khi có broker cần cập nhật sketch Wokwi.
- Chống trùng MQTT trong script là ở bộ nhớ phiên chạy, chưa bảo đảm exactly-once qua việc ESP32 khởi động lại hay máy Python restart.
- Phân tích hiệu năng lưu trữ và đo end-to-end chính xác cần thêm bước thực nghiệm/báo cáo sau.
