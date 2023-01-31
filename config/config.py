SENSOR_PIN_DHT = 14 # pin D5 on D1 mini
SENSOR_INTERVAL = 15 # in seconds
TEMP_THRESHOLD = 0.2 # temp change has to be >=
HUM_THRESHOLD = 0.5 # hum change has to be >=

# -1 unlimited skips
# 0 forces to send every time
# >= 1 set the max skip count
MAX_PUBLISH_SKIPS = -1

MQTT_SERVER = '192.168.2.123'
SENSOR_TOPIC = 'sensor/raw'
