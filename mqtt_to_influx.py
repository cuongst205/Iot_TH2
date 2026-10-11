
import json
import math
import time
from collections import deque
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from config import (
    influx_client,
    INFLUX_ORG,
    RAW_BUCKET,
    PROCESSED_BUCKET,
    MQTT_HOST,
    MQTT_PORT,
    MQTT_USERNAME,
    MQTT_PASSWORD,
    MQTT_TOPIC,
    MQTT_USE_TLS,
)

# ==========================================
# CAU HINH
# ==========================================

WINDOW_SIZE = 12

# Luu 12 mau gan nhat cho moi thiet bi
windows = {}

# Chong trung lap trong phien chay
seen = set()
last_sequence = {}

counts = {
    "received": 0,
    "saved": 0,
    "invalid": 0,
    "duplicate": 0,
    "missing": 0,
    "errors": 0,
}

write_api = None


# ==========================================
# KIEM TRA DU LIEU
# ==========================================

def validate(data):
    if not isinstance(data, dict):
        raise ValueError("JSON khong hop le")

    device = data.get("device_id")

    if not isinstance(device, str) or not device:
        raise ValueError("device_id khong hop le")

    def number(key, low, high):
        value = data.get(key)

        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
        ):
            raise ValueError(f"{key} phai la so")

        if (
            not math.isfinite(value)
            or not low <= value <= high
        ):
            raise ValueError(f"{key} ngoai pham vi")

        return value

    boot = data.get("boot_id", "legacy")

    if (
        isinstance(boot, bool)
        or not isinstance(boot, (str, int))
    ):
        raise ValueError("boot_id khong hop le")

    seq = number("sequence", 1, 2147483647)
    sent = number(
        "sent_at_ms",
        1700000000000,
        4102444800000
    )

    if not float(seq).is_integer():
        raise ValueError("sequence phai la so nguyen")

    if not float(sent).is_integer():
        raise ValueError("sent_at_ms phai la so nguyen")

    return {
        "device_id": device,
        "boot_id": str(boot),
        "temperature": float(
            number("temperature", -40, 80)
        ),
        "humidity": float(
            number("humidity", 0, 100)
        ),
        "distance_cm": float(
            number("distance_cm", 0, 500)
        ),
        "rssi": int(number("rssi", -120, 0)),
        "sequence": int(seq),
        "uptime_s": int(
            number("uptime_s", 0, 1000000000)
        ),
        "sent_at_ms": int(sent),
    }


# ==========================================
# HAM TIEN XU LY REALTIME
# ==========================================

def normalize(value, values):
    lowest = min(values)
    highest = max(values)

    if highest == lowest:
        return 0.0

    return (value - lowest) / (highest - lowest)


def preprocess_realtime(data):
    device = data["device_id"]

    if device not in windows:
        windows[device] = deque(maxlen=WINDOW_SIZE)

    history = windows[device]

    temperature = data["temperature"]
    humidity = data["humidity"]

    previous = (
        history[-1]["temperature"]
        if history
        else temperature
    )

    # Tao cua so tam, chi cap nhat sau khi ghi DB OK
    samples = list(history) + [{
        "temperature": temperature,
        "humidity": humidity,
    }]

    samples = samples[-WINDOW_SIZE:]

    temps = [
        x["temperature"] for x in samples
    ]

    hums = [
        x["humidity"] for x in samples
    ]

    result = {
        "temperature": temperature,
        "humidity": humidity,
        "distance_cm": data["distance_cm"],
        "temperature_rolling_mean": (
            sum(temps) / len(temps)
        ),
        "humidity_rolling_mean": (
            sum(hums) / len(hums)
        ),
        "temperature_delta": (
            temperature - previous
        ),
        "temperature_norm": normalize(
            temperature, temps
        ),
        "humidity_norm": normalize(
            humidity, hums
        ),
    }

    return result, samples


# ==========================================
# MQTT CALLBACKS
# ==========================================

def on_connect(
    client, userdata, flags,
    reason_code, properties
):
    if reason_code == 0:
        client.subscribe(MQTT_TOPIC, qos=1)
        print("MQTT connected:", MQTT_TOPIC)
    else:
        print("MQTT connection failed:", reason_code)


def on_message(client, userdata, message):
    start_ns = time.perf_counter_ns()
    received_ms = time.time_ns() / 1_000_000

    counts["received"] += 1

    try:
        data = validate(
            json.loads(message.payload.decode("utf-8"))
        )

        device = data["device_id"]
        boot = data["boot_id"]
        seq = data["sequence"]

        key = (device, boot, seq)

        if key in seen:
            counts["duplicate"] += 1
            print("[DUPLICATE]", seq)
            return

        timestamp = datetime.fromtimestamp(
            received_ms / 1000,
            timezone.utc
        )

        # ==================================
        # 1. DU LIEU GOC
        # ==================================

        raw_point = (
            Point("sensor_data")
            .tag("device_id", device)
            .tag("boot_id", boot)
            .field(
                "temperature",
                data["temperature"]
            )
            .field(
                "humidity",
                data["humidity"]
            )
            .field(
                "distance_cm",
                data["distance_cm"]
            )
            .field(
                "rssi",
                data["rssi"]
            )
            .field(
                "sequence",
                seq
            )
            .field(
                "uptime_s",
                data["uptime_s"]
            )
            .field(
                "sent_at_ms",
                data["sent_at_ms"]
            )
            .time(timestamp, WritePrecision.NS)
        )

        # ==================================
        # 2. TU DONG TIEN XU LY
        # ==================================

        processed, new_window = preprocess_realtime(
            data
        )

        processed_point = (
            Point("sensor_processed")
            .tag("device_id", device)
            .tag("boot_id", boot)
            .time(timestamp, WritePrecision.NS)
        )

        for field, value in processed.items():
            processed_point.field(
                field,
                float(value)
            )

        # Ghi raw va processed vao hai bucket
        write_api.write(
            bucket=RAW_BUCKET,
            org=INFLUX_ORG,
            record=raw_point
        )

        write_api.write(
            bucket=PROCESSED_BUCKET,
            org=INFLUX_ORG,
            record=processed_point
        )

        # Chi cap nhat rolling window sau khi ghi OK
        windows[device] = deque(
            new_window,
            maxlen=WINDOW_SIZE
        )

        # ==================================
        # 3. DO DO TRE
        # ==================================

        completed_ms = (
            time.time_ns() / 1_000_000
        )

        processing_ms = (
            time.perf_counter_ns() - start_ns
        ) / 1_000_000

        transport_ms = (
            received_ms - data["sent_at_ms"]
        )

        end_to_end_ms = (
            completed_ms - data["sent_at_ms"]
        )

        # ==================================
        # 4. KIEM TRA SEQUENCE
        # ==================================

        session = (device, boot)
        previous = last_sequence.get(session)

        if previous is not None and seq > previous + 1:
            counts["missing"] += (
                seq - previous - 1
            )

        if previous is None or seq > previous:
            last_sequence[session] = seq

        seen.add(key)
        counts["saved"] += 1

        # ==================================
        # 5. LUU DO TRE VAO INFLUXDB
        # ==================================

        metrics = (
            Point("pipeline_metrics")
            .tag("device_id", device)
            .tag("boot_id", boot)
            .field(
                "processing_ms",
                float(processing_ms)
            )
            .field(
                "sequence",
                seq
            )
            .field(
                "saved_total",
                counts["saved"]
            )
            .field(
                "missing_total",
                counts["missing"]
            )
            .field(
                "duplicate_total",
                counts["duplicate"]
            )
            .time(timestamp, WritePrecision.NS)
        )

        # Chi ghi do tre dua tren NTP khi hop le
        if (
            0 <= transport_ms <= 120000
            and end_to_end_ms >= 0
        ):
            metrics.field(
                "transport_ms",
                float(transport_ms)
            )
            metrics.field(
                "end_to_end_ms",
                float(end_to_end_ms)
            )
        else:
            print(
                "[CLOCK WARNING] NTP offset:",
                f"{transport_ms:.2f} ms"
            )

        write_api.write(
            bucket=RAW_BUCKET,
            org=INFLUX_ORG,
            record=metrics
        )

        print(
            f"[OK] seq={seq} | "
            f"T={data['temperature']:.1f} C | "
            f"H={data['humidity']:.1f}% | "
            f"D={data['distance_cm']:.1f} cm | "
            f"E2E={end_to_end_ms:.1f} ms | "
            f"Processed=OK"
        )

        if counts["saved"] % 10 == 0:
            print("STATS:", counts)

    except (
        ValueError,
        TypeError,
        KeyError,
        UnicodeDecodeError
    ) as exc:
        counts["invalid"] += 1
        print("[INVALID]", exc)

    except Exception as exc:
        counts["errors"] += 1
        print("[ERROR]", exc)


# ==========================================
# MAIN
# ==========================================

def main():
    global write_api

    with influx_client() as influx:
        if not influx.ping():
            raise RuntimeError(
                "InfluxDB chua hoat dong"
            )

        write_api = influx.write_api(
            write_options=SYNCHRONOUS
        )

        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"iot-realtime-{time.time_ns()}"
        )

        if MQTT_USERNAME:
            client.username_pw_set(
                MQTT_USERNAME,
                MQTT_PASSWORD
            )

        if MQTT_USE_TLS:
            client.tls_set()

        client.on_connect = on_connect
        client.on_message = on_message

        client.reconnect_delay_set(
            min_delay=1,
            max_delay=30
        )

        print("Connecting MQTT...")
        client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=60
        )

        try:
            client.loop_forever()

        except KeyboardInterrupt:
            print("\nStopped")
            print("Statistics:", counts)

        finally:
            client.disconnect()


if __name__ == "__main__":
    main()
