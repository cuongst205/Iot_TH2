"""Ghi thu 1 diem du lieu vao InfluxDB 2.x va doc lai."""
from datetime import datetime, timezone
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from config import influx_client, INFLUX_ORG, RAW_BUCKET


def main():
    with influx_client() as client:
        if not client.ping():
            raise RuntimeError('Khong ket noi duoc InfluxDB. Kiem tra influxd.exe / cong 8086.')
        print('Da ket noi InfluxDB!')
        point = (Point('sensor_data')
                 .tag('device_id', 'esp32_test')
                 .field('temperature', 25.5)
                 .field('humidity', 60.2)
                 .field('distance_cm', 200.5)
                 .field('rssi', -84)
                 .field('sequence', 1)
                 .time(datetime.now(timezone.utc), WritePrecision.NS))
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=RAW_BUCKET, org=INFLUX_ORG, record=point
        )
        print(f'Da ghi 1 ban ghi thu vao bucket {RAW_BUCKET}.')
        query = f'''from(bucket: "{RAW_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r._measurement == "sensor_data" and r.device_id == "esp32_test")
          |> filter(fn: (r) => r._field == "temperature")
          |> last()'''
        tables = client.query_api().query(query, org=INFLUX_ORG)
        for table in tables:
            for record in table.records:
                print('Doc lai nhiet do:', record.get_value(), 'tai', record.get_time())


if __name__ == '__main__':
    main()
