#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <time.h>
#include <sys/time.h>

const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";
const char* MQTT_SERVER = "broker.hivemq.com";
const int MQTT_PORT = 1883;
const char* MQTT_TOPIC = "ptit/iot/th2/esp32_01";
const int DHT_PIN = 15, TRIG_PIN = 5, ECHO_PIN = 18, LED_PIN = 2;
const unsigned long SEND_INTERVAL_MS = 5000;
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
DHTesp dht;
unsigned long lastSend = 0;
unsigned long sequenceNo = 0;
String bootId;

void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
  Serial.print("Connecting WiFi");
  while (WiFi.status() != WL_CONNECTED) { delay(500); Serial.print("."); }
  Serial.println(" connected");
}

void syncClock() {
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  Serial.print("Synchronizing UTC clock");
  for (int i = 0; i < 30 && time(nullptr) < 1700000000; i++) {
    delay(500);
    Serial.print(".");
  }
  Serial.println(time(nullptr) >= 1700000000 ? " OK" : " FAILED (retry later)");
}

void connectMQTT() {
  while (!mqttClient.connected()) {
    String clientId = "ESP32-TH2-" + bootId;
    Serial.print("Connecting MQTT...");
    if (mqttClient.connect(clientId.c_str())) Serial.println(" connected");
    else { Serial.printf(" failed rc=%d\n", mqttClient.state()); delay(2000); }
  }
}

float readDistanceCm() {
  digitalWrite(TRIG_PIN, LOW); delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH); delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);
  unsigned long duration = pulseIn(ECHO_PIN, HIGH, 30000);
  return duration == 0 ? NAN : duration * 0.0343f / 2.0f;
}

void setup() {
  Serial.begin(115200);
  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
  dht.setup(DHT_PIN, DHTesp::DHT22);
  bootId = String((uint32_t)ESP.getEfuseMac(), HEX) + "-" + String(esp_random(), HEX);
  connectWiFi();
  syncClock();
  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
  mqttClient.setBufferSize(384);
  Serial.println("ESP32 IoT TH2 ready");
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectWiFi();
  if (!mqttClient.connected()) connectMQTT();
  mqttClient.loop();
  unsigned long now = millis();
  if (now - lastSend < SEND_INTERVAL_MS) return;
  lastSend = now;
  TempAndHumidity data = dht.getTempAndHumidity();
  float distance = readDistanceCm();
  if (!isfinite(data.temperature) || !isfinite(data.humidity) || !isfinite(distance)) {
    Serial.println("Invalid sensor data - skipped");
    return;
  }
  if (time(nullptr) < 1700000000) {
    syncClock();
    if (time(nullptr) < 1700000000) { Serial.println("No valid UTC time - skipped"); return; }
  }
  sequenceNo++;
  // Read the sending timestamp immediately before formatting the MQTT payload.
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  int64_t sentAtMs = (int64_t)tv.tv_sec * 1000LL + tv.tv_usec / 1000;
  char payload[384];
  snprintf(payload, sizeof(payload),
    "{\"device_id\":\"esp32_01\",\"boot_id\":\"%s\",\"temperature\":%.2f,"
    "\"humidity\":%.2f,\"distance_cm\":%.2f,\"rssi\":%d,"
    "\"sequence\":%lu,\"uptime_s\":%lu,\"sent_at_ms\":%lld}",
    bootId.c_str(), data.temperature, data.humidity, distance, WiFi.RSSI(),
    sequenceNo, millis() / 1000, (long long)sentAtMs);
  bool ok = mqttClient.publish(MQTT_TOPIC, payload);
  Serial.printf("%s | publish=%s\n", payload, ok ? "OK" : "FAILED");
  digitalWrite(LED_PIN, HIGH); delay(80); digitalWrite(LED_PIN, LOW);
}
