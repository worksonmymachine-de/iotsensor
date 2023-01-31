import dht
from machine import Pin
from time import sleep
import config.config as cfg
import config.wifi_credentials as cred
import network
import ubinascii
import json as j
from umqtt.robust import MQTTClient

class IoTTempHumSensorThresholded:
    
    def __init__(self):
        self.temp = None
        self.hum = None
        self.prev_temp = None
        self.prev_hum = None
        self.temp_hum_sensor = dht.DHT22(Pin(cfg.SENSOR_PIN_DHT, Pin.IN))
        self.wifi = network.WLAN(network.STA_IF)
        self.id = ubinascii.hexlify(self.wifi.config('mac'), ":").decode().upper()
        self.publishes_skipped = 0
        print(f'id = {self.id}')
    
    def connect_to_wifi(self):
        print('connecting to wifi')
        self.wifi.active(True)
        self.wifi.connect(cred.WIFI_SSID, cred.WIFI_PASSWORD)
        while not self.wifi.isconnected():
            sleep(0.3)
        print(f'connected to wifi {self.wifi.ifconfig()}')
                
    def read_sensors(self):
        self.temp_hum_sensor.measure()
        self.temp = self.temp_hum_sensor.temperature()
        self.hum = self.temp_hum_sensor.humidity()
        
    def should_publish(self) -> bool:
        will_publish = True # default - will be changed if no threshold exceeded
        if self.prev_temp is None or self.prev_hum is None:
            print('No previous value')
        elif cfg.MAX_PUBLISH_SKIPS == 0:
            pass
        elif self.max_skipped():
            print('Max times skipped reached')
        elif abs(self.temp - self.prev_temp) >= cfg.TEMP_THRESHOLD:
            print('Temperature change threshold exceeded')
        elif abs(self.hum - self.prev_hum) >= cfg.HUM_THRESHOLD:
            print('Humidity change threshold exceeded')
        else:
            print(f'Skipping update: Temp: {self.temp} Hum: {self.hum}')
            will_publish = False
        self.publishes_skipped = 0 if will_publish else self.publishes_skipped + 1
        return will_publish
        
    def max_skipped(self) -> bool:
        return False if cfg.MAX_PUBLISH_SKIPS == -1 else self.publishes_skipped > cfg.MAX_PUBLISH_SKIPS
        
    def update_previous_values(self) -> None:
        self.prev_temp = self.temp
        self.prev_hum = self.hum
    
    def publish(self):
        mqtt_client = MQTTClient(self.id, cfg.MQTT_SERVER, keepalive=5)
        mqtt_client.connect()
        data = j.dumps({'temp': self.temp, 'hum': self.hum, 'id': self.id})
        mqtt_client.publish(cfg.SENSOR_TOPIC, data)
        print(f"====> Published: Temp: {self.temp} Hum: {self.hum}")
        
    def run(self):
        while True:
            try:
                self.read_sensors()
                if(self.should_publish()):
                    if not self.wifi.isconnected():
                        self.connect_to_wifi()
                    else:
                        self.update_previous_values()
                        self.publish()
            except Exception as e:
                print(f"Error when reading sensors or publishing - continuing\n{e}")
            sleep(cfg.SENSOR_INTERVAL)

if __name__ == "__main__":
    IoTTempHumSensorThresholded().run()
