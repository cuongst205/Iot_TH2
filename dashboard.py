
import pandas as pd
import streamlit as st
from datetime import datetime, timezone, timedelta

from config import (
    influx_client,
    RAW_BUCKET,
    PROCESSED_BUCKET,
)

# ==================================================
# CAU HINH TRANG
# ==================================================

st.set_page_config(
    page_title="IoT TH2 - Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 DASHBOARD GIÁM SÁT HỆ THỐNG IoT")
st.caption(
    "ESP32 Wokwi → MQTT → Python → InfluxDB → Streamlit"
)

with st.sidebar:
    st.header("⚙️ Cấu hình")

    device_id = st.text_input(
        "Mã thiết bị",
        value="esp32_01"
    )

    hours = st.selectbox(
        "Khoảng thời gian",
        [1, 3, 6, 12, 24],
        index=4,
        format_func=lambda x: f"{x} giờ"
    )

    st.info("Dashboard tự cập nhật mỗi 5 giây.")


# ==================================================
# HAM TRUY VAN INFLUXDB
# ==================================================

def load_data(client, bucket, measurement, hours, device):
    # json.dumps giup tao string Flux an toan
    import json

    bucket_name = json.dumps(bucket)
    measurement_name = json.dumps(measurement)
    device_name = json.dumps(device)

    query = f'''
from(bucket: {bucket_name})
  |> range(start: -{int(hours)}h)
  |> filter(fn: (r) =>
    r._measurement == {measurement_name})
  |> filter(fn: (r) =>
    r.device_id == {device_name})
  |> pivot(
    rowKey: ["_time"],
    columnKey: ["_field"],
    valueColumn: "_value"
  )
  |> sort(columns: ["_time"])
'''

    result = client.query_api().query_data_frame(query)

    if isinstance(result, list):
        frames = [
            x for x in result
            if isinstance(x, pd.DataFrame) and not x.empty
        ]

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
    else:
        df = result

    if df is None or df.empty:
        return pd.DataFrame()

    df["_time"] = pd.to_datetime(
        df["_time"],
        utc=True,
        errors="coerce"
    )

    df = df.dropna(subset=["_time"])
    df = df.sort_values("_time")

    return df


def values(df, field):
    if df.empty or field not in df.columns:
        return pd.Series(dtype="float64")

    return pd.to_numeric(
        df[field],
        errors="coerce"
    ).dropna()


def latest(df, field, unit="", digits=2):
    series = values(df, field)

    if series.empty:
        return "N/A"

    return f"{series.iloc[-1]:.{digits}f}{unit}"


def draw_chart(df, fields, height=220):
    if df.empty:
        st.info("Chưa có dữ liệu.")
        return

    available = [
        name for name in fields
        if name in df.columns
    ]

    if not available:
        st.info("Chưa có trường dữ liệu phù hợp.")
        return

    plot = df[["_time"] + available].copy()

    for name in available:
        plot[name] = pd.to_numeric(
            plot[name],
            errors="coerce"
        )

    plot = plot.set_index("_time").sort_index()

    # Gop cac ban ghi cung timestamp (neu co)
    plot = plot.groupby(level=0).mean()

    st.line_chart(
        plot[available],
        height=height,
        use_container_width=True
    )


def retention(client, bucket_name):
    bucket = (
        client.buckets_api()
        .find_bucket_by_name(bucket_name)
    )

    if bucket is None:
        return "Không tìm thấy"

    rules = getattr(
        bucket,
        "retention_rules",
        None
    ) or []

    if not rules:
        return "Không giới hạn"

    first_rule = rules[0]

    seconds = getattr(
        first_rule,
        "every_seconds",
        None
    )

    if seconds is None:
        seconds = getattr(
            first_rule,
            "every",
            0
        )

    seconds = int(seconds or 0)

    if seconds == 0:
        return "Không giới hạn"

    return f"{seconds / 86400:g} ngày"


def storage_stats(df):
    if df.empty:
        return 0, None, 0.0

    # Moi diem sensor_data co truong temperature
    valid = df.copy()

    if "temperature" in valid.columns:
        valid = valid[
            pd.to_numeric(
                valid["temperature"],
                errors="coerce"
            ).notna()
        ]

    count = len(valid)

    if count == 0:
        return 0, None, 0.0

    times = (
        valid["_time"]
        .sort_values()
        .drop_duplicates()
    )

    gaps = (
        times.diff()
        .dt.total_seconds()
        .dropna()
    )

    avg_gap = (
        float(gaps.mean())
        if not gaps.empty
        else None
    )

    cutoff = (
        pd.Timestamp.now(tz="UTC")
        - pd.Timedelta(minutes=10)
    )

    recent_count = int(
        (valid["_time"] >= cutoff).sum()
    )

    # So diem quan sat duoc trong 10 phut
    rate = recent_count / 10.0

    return count, avg_gap, rate


def latency_summary(df, field, label):
    series = values(df, field)

    if series.empty:
        st.caption(f"{label}: Chưa có mẫu")
        return

    st.caption(
        f"{label}: "
        f"TB {series.mean():.2f} ms | "
        f"Min {series.min():.2f} | "
        f"Max {series.max():.2f} | "
        f"n={len(series)}"
    )


# ==================================================
# DASHBOARD 2x2 - AUTO REFRESH
# ==================================================

@st.fragment(run_every="5s")
def refresh_dashboard():

    try:
        with influx_client() as client:
            if not client.ping():
                st.error("Không kết nối được InfluxDB.")
                return

            raw = load_data(
                client,
                RAW_BUCKET,
                "sensor_data",
                hours,
                device_id
            )

            metrics = load_data(
                client,
                RAW_BUCKET,
                "pipeline_metrics",
                hours,
                device_id
            )

            processed = load_data(
                client,
                PROCESSED_BUCKET,
                "sensor_processed",
                hours,
                device_id
            )

            raw_retention = retention(
                client,
                RAW_BUCKET
            )

            processed_retention = retention(
                client,
                PROCESSED_BUCKET
            )

        st.caption(
            "🟢 InfluxDB đang kết nối | "
            "Cập nhật lúc "
            f"{datetime.now().strftime('%H:%M:%S')}"
        )

        # Tao luoi 2 cot, 2 hang
        col_left, col_right = st.columns(
            2,
            gap="medium"
        )

        # =========================================
        # O 1 - CAM BIEN
        # =========================================

        with col_left:
            with st.container(border=True):

                st.subheader("🌡️ 1. Giám sát cảm biến")

                a, b, c = st.columns(3)

                a.metric(
                    "Nhiệt độ",
                    latest(raw, "temperature", " °C")
                )

                b.metric(
                    "Độ ẩm",
                    latest(raw, "humidity", " %")
                )

                c.metric(
                    "Khoảng cách",
                    latest(raw, "distance_cm", " cm")
                )

                st.markdown("**Biểu đồ cảm biến**")

                draw_chart(
                    raw,
                    [
                        "temperature",
                        "humidity",
                        "distance_cm"
                    ]
                )

                if not raw.empty:
                    st.caption(
                        f"Số điểm dữ liệu: {len(raw)}"
                    )

                with st.expander("Xem bảng dữ liệu"):
                    st.dataframe(
                        raw,
                        use_container_width=True
                    )

        # =========================================
        # O 2 - DO TRE
        # =========================================

        with col_right:
            with st.container(border=True):

                st.subheader("⏱️ 2. Độ trễ realtime")

                a, b, c = st.columns(3)

                a.metric(
                    "MQTT",
                    latest(
                        metrics,
                        "transport_ms",
                        " ms"
                    )
                )

                b.metric(
                    "Python → DB",
                    latest(
                        metrics,
                        "processing_ms",
                        " ms"
                    )
                )

                c.metric(
                    "End-to-end",
                    latest(
                        metrics,
                        "end_to_end_ms",
                        " ms"
                    )
                )

                st.markdown("**Biểu đồ độ trễ**")

                draw_chart(
                    metrics,
                    [
                        "transport_ms",
                        "processing_ms",
                        "end_to_end_ms"
                    ]
                )

                latency_summary(
                    metrics,
                    "transport_ms",
                    "MQTT → Python"
                )

                latency_summary(
                    metrics,
                    "processing_ms",
                    "Python → DB"
                )

                latency_summary(
                    metrics,
                    "end_to_end_ms",
                    "End-to-end"
                )

                with st.expander("Xem mẫu đo độ trễ"):
                    st.dataframe(
                        metrics,
                        use_container_width=True
                    )

        # Tao hang thu hai
        col_bottom_left, col_bottom_right = st.columns(
            2,
            gap="medium"
        )

        # =========================================
        # O 3 - LUU TRU
        # =========================================

        with col_bottom_left:
            with st.container(border=True):

                st.subheader("💾 3. Hiệu năng lưu trữ")

                raw_count, avg_gap, rate = storage_stats(
                    raw
                )

                processed_count, _, _ = storage_stats(
                    processed
                )

                a, b, c = st.columns(3)

                a.metric(
                    "Dữ liệu gốc",
                    raw_count
                )

                b.metric(
                    "Đã xử lý",
                    processed_count
                )

                c.metric(
                    "Điểm/phút",
                    f"{rate:.2f}"
                )

                st.markdown("**Retention Policy**")

                a, b = st.columns(2)

                a.metric(
                    RAW_BUCKET,
                    raw_retention
                )

                b.metric(
                    PROCESSED_BUCKET,
                    processed_retention
                )

                if avg_gap is not None:
                    st.metric(
                        "Khoảng cách TB giữa các bản ghi",
                        f"{avg_gap:.2f} giây"
                    )
                else:
                    st.metric(
                        "Khoảng cách TB giữa các bản ghi",
                        "N/A"
                    )

                st.caption(
                    "Số điểm trong khoảng thời gian "
                    "đã chọn. Tốc độ ghi quan sát được "
                    "tính trong 10 phút gần nhất, "
                    "không phải hiệu năng tối đa của ổ đĩa."
                )

                st.caption(
                    "Dung lượng đĩa thực tế "
                    "chưa được đo."
                )

        # =========================================
        # O 4 - TIEN XU LY
        # =========================================

        with col_bottom_right:
            with st.container(border=True):

                st.subheader("📈 4. Dữ liệu tiền xử lý")

                if processed.empty:
                    st.warning(
                        "Chưa có dữ liệu xử lý. "
                        "Chạy python preprocess.py "
                        "để cập nhật dữ liệu."
                    )

                else:
                    st.markdown(
                        "**Nhiệt độ và trung bình trượt**"
                    )

                    draw_chart(
                        processed,
                        [
                            "temperature",
                            "temperature_rolling_mean"
                        ],
                        height=185
                    )

                    st.markdown(
                        "**Dữ liệu chuẩn hóa**"
                    )

                    draw_chart(
                        processed,
                        [
                            "temperature_norm",
                            "humidity_norm"
                        ],
                        height=185
                    )

                    st.caption(
                        f"Số điểm đã xử lý: "
                        f"{processed_count}"
                    )

                    with st.expander(
                        "Xem bảng tiền xử lý"
                    ):
                        st.dataframe(
                            processed,
                            use_container_width=True
                        )

        st.caption(
            "Lưu ý: Độ trễ end-to-end là ước tính "
            "dựa trên đồng hồ NTP của ESP32. "
            "Dữ liệu đã xử lý chỉ đổi khi "
            "preprocess.py được chạy lại."
        )

    except Exception as exc:
        st.error(
            f"Lỗi cập nhật Dashboard: {exc}"
        )


refresh_dashboard()
