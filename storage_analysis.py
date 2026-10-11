
"""Phan tich luu tru va retention InfluxDB IoT TH2."""

import csv
import json

from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from config import (
    influx_client,
    RAW_BUCKET,
    PROCESSED_BUCKET,
)

OUT = (
    Path(__file__).resolve().parent
    / "reports"
)

OUT.mkdir(
    parents=True,
    exist_ok=True
)


def flux_string(value):
    return json.dumps(
        value,
        ensure_ascii=False
    )


# ===== RETENTION =====

def describe_bucket(client, name):
    bucket = (
        client.buckets_api()
        .find_bucket_by_name(name)
    )

    if bucket is None:
        print(
            f"[WARNING] Bucket not found: {name}"
        )

        return {
            "bucket": name,
            "retention_days": "NOT FOUND",
            "retention_seconds": "",
        }

    rules = (
        getattr(bucket, "retention_rules", None)
        or []
    )

    seconds = (
        int(rules[0].every_seconds)
        if rules
        else 0
    )

    label = (
        "Forever"
        if seconds == 0
        else f"{seconds / 86400:g} days"
    )

    print(
        f"[RETENTION] {name}: {label}"
    )

    return {
        "bucket": name,
        "retention_days": label,
        "retention_seconds": seconds,
    }


# ===== TRUY VAN DU LIEU =====

def get_rows(
    client,
    bucket,
    measurement,
    hours=24
):
    query = f'''
from(bucket: {flux_string(bucket)})
  |> range(start: -{hours}h)
  |> filter(fn: (r) =>
      r._measurement == {flux_string(measurement)})
  |> filter(fn: (r) =>
      r._field == "temperature")
  |> keep(columns: ["_time", "_value", "device_id"])
  |> sort(columns: ["_time"])
'''

    rows = []

    tables = client.query_api().query(
        query
    )

    for table in tables:
        for record in table.records:
            rows.append({
                "time": record.get_time(),
                "temperature": float(
                    record.get_value()
                ),
                "device_id": record.values.get(
                    "device_id", ""
                ),
            })

    rows.sort(
        key=lambda r: r["time"]
    )

    return rows


# ===== THONG KE =====

def summarize(
    name,
    rows,
    hours=24
):
    times = [
        r["time"]
        for r in rows
    ]

    spans = [
        (b - a).total_seconds()
        for a, b in zip(
            times,
            times[1:]
        )
        if b >= a
    ]

    item = {
        "bucket": name,
        "measurement_points_24h": len(rows),
        "first_time_utc": (
            times[0].isoformat()
            if times
            else ""
        ),
        "last_time_utc": (
            times[-1].isoformat()
            if times
            else ""
        ),
        "observed_avg_gap_seconds": (
            round(mean(spans), 3)
            if spans
            else ""
        ),
        "measured_window_hours": hours,
    }

    print(
        f"[DATA] {name}: "
        f"{len(rows)} diem temperature / {hours}h"
    )

    if times:
        print(
            "       Tu:",
            item["first_time_utc"]
        )

        print(
            "       Den:",
            item["last_time_utc"]
        )

        print(
            "       Khoang cach trung binh:",
            item["observed_avg_gap_seconds"],
            "s"
        )

    return item


# ===== GHI CSV =====

def save_csv(file, rows):
    if not rows:
        return

    keys = list(
        rows[0].keys()
    )

    with file.open(
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=keys
        )

        writer.writeheader()
        writer.writerows(rows)


# ===== CHUONG TRINH CHINH =====

def main():
    print(
        "=== PHAN TICH LUU TRU "
        "INFLUXDB - IoT TH2 ==="
    )

    print(
        "Thoi gian kiem tra (UTC):",
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    with influx_client() as client:
        if not client.ping():
            raise RuntimeError(
                "InfluxDB khong phan hoi."
            )

        retention = [
            describe_bucket(
                client,
                RAW_BUCKET
            ),
            describe_bucket(
                client,
                PROCESSED_BUCKET
            ),
        ]

        raw = summarize(
            RAW_BUCKET,
            get_rows(
                client,
                RAW_BUCKET,
                "sensor_data"
            )
        )

        processed = summarize(
            PROCESSED_BUCKET,
            get_rows(
                client,
                PROCESSED_BUCKET,
                "sensor_processed"
            )
        )

    print("\n[DUNG LUONG]")
    print(
        "Khong the tinh dung luong thuc te "
        "chi tu so ban ghi."
    )

    print(
        "Muốn do dung luong tren dia, "
        "kiem tra thu muc engine cua InfluxDB."
    )

    print(
        "Luu y: Khoang cach trung binh "
        "khong phai do tre MQTT hay ty le mat goi."
    )

    save_csv(
        OUT / "retention_report.csv",
        retention
    )

    save_csv(
        OUT / "storage_report.csv",
        [raw, processed]
    )

    print("\nDa tao:")
    print(
        " -",
        OUT / "retention_report.csv"
    )
    print(
        " -",
        OUT / "storage_report.csv"
    )


if __name__ == "__main__":
    main()
