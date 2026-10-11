"""Xu ly du lieu thieu, outlier IQR, resampling, rolling mean, delta, min-max."""
import argparse
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from config import influx_client, INFLUX_ORG, RAW_BUCKET, PROCESSED_BUCKET

FIELDS = ['temperature', 'humidity', 'distance_cm']


def iqr_clean(series):
    q1, q3 = series.quantile([0.25, 0.75])
    iqr = q3 - q1
    if pd.isna(iqr) or iqr == 0:
        return series
    return series.mask((series < q1 - 1.5 * iqr) | (series > q3 + 1.5 * iqr))


def process_device(frame):
    frame = frame.sort_index()
    for name in FIELDS:
        if name not in frame:
            frame[name] = np.nan
        frame[name] = pd.to_numeric(frame[name], errors='coerce')
    # Cua so 30 giay: resample -> phat hien outlier -> noi suy gia tri thieu
    result = frame[FIELDS].resample('30s').mean()
    for name in FIELDS:
        result[name] = iqr_clean(result[name]).interpolate(method='time', limit_direction='both')
        result[f'{name}_rolling_mean'] = result[name].rolling(3, min_periods=1).mean()
        result[f'{name}_delta'] = result[name].diff().fillna(0)
        min_v, max_v = result[name].min(), result[name].max()
        if pd.isna(min_v) or pd.isna(max_v):
            result[f'{name}_norm'] = np.nan
        elif max_v == min_v:
            result[f'{name}_norm'] = 0.0
        else:
            result[f'{name}_norm'] = (result[name] - min_v) / (max_v - min_v)
    return result


def main(hours):
    with influx_client() as client:
        query = f'''from(bucket: "{RAW_BUCKET}")
          |> range(start: -{hours}h)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity" or r._field == "distance_cm")'''
        tables = client.query_api().query(query, org=INFLUX_ORG)
        rows = []
        for table in tables:
            for r in table.records:
                rows.append({'time': r.get_time(), 'device_id': r.values.get('device_id', 'esp32_01'),
                             'field': r.get_field(), 'value': r.get_value()})
        if not rows:
            print('Khong co du lieu trong thoi gian da chon. Chay mqtt_to_influx.py truoc.')
            return
        df = pd.DataFrame(rows)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        writer = client.write_api(write_options=SYNCHRONOUS)
        total = 0
        for device, subset in df.groupby('device_id'):
            wide = (subset.pivot_table(index='time', columns='field', values='value', aggfunc='mean')
                    .sort_index())
            processed = process_device(wide)
            points = []
            for timestamp, row in processed.iterrows():
                point = (Point('sensor_processed').tag('device_id', device)
                         .time(timestamp.to_pydatetime(), WritePrecision.NS))
                nfields = 0
                for name, value in row.items():
                    if pd.notna(value) and np.isfinite(float(value)):
                        point.field(name, float(value))
                        nfields += 1
                if nfields:
                    points.append(point)
            if points:
                writer.write(bucket=PROCESSED_BUCKET, org=INFLUX_ORG, record=points)
                total += len(points)
        print(f'Tien xu ly va ghi {total} diem vao bucket {PROCESSED_BUCKET}.')
        print('Luu y: Chay lai script ghi de cac timestamp trung; khong chong lap theo phien boot.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--hours', type=int, default=24, help='So gio du lieu muon xu ly')
    args = parser.parse_args()
    if not 1 <= args.hours <= 720:
        raise SystemExit('--hours phai nam trong khoang 1..720')
    main(args.hours)
