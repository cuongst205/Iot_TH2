"""Cau hinh chung cho cac script IoT TH2."""
import os
from dotenv import load_dotenv

load_dotenv()

INFLUX_URL = os.getenv('INFLUX_URL', 'http://localhost:8086')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN', '')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'PTIT')
RAW_BUCKET = os.getenv('INFLUX_BUCKET', 'iot_th2')
PROCESSED_BUCKET = os.getenv('INFLUX_PROCESSED_BUCKET', 'iot_processed')

MQTT_HOST = os.getenv('MQTT_HOST', '')
MQTT_PORT = int(os.getenv('MQTT_PORT', '8883'))
MQTT_USERNAME = os.getenv('MQTT_USERNAME', '')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', '')
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'iot/esp32_01/sensors')
MQTT_USE_TLS = os.getenv('MQTT_USE_TLS', 'true').strip().lower() in ('true', '1', 'yes')


def require_influx_config():
    if not INFLUX_TOKEN or INFLUX_TOKEN == 'DIEN_TOKEN_CUA_BAN':
        raise RuntimeError('Chua dien INFLUX_TOKEN trong file .env')


def influx_client():
    from influxdb_client import InfluxDBClient
    require_influx_config()
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30000)
