import Adafruit_DHT
from time import sleep
import asyncio
from paho.mqtt import subscribe, publish
from paho.mqtt.client import MQTTMessage
from typing import Awaitable, Any
import uuid
from json.decoder import JSONDecoder
from json.encoder import JSONEncoder

JSON = 0
STRING = 1
INT = 2
_id = uuid.getnode()

async def run_sequence(*functions: Awaitable[Any]) -> None:
    for function in functions:
        await function


async def run_parallel(*functions: Awaitable[Any]) -> None:
    await asyncio.gather(*functions)
    

def build_json(values):
    return JSONEncoder().encode(values)


class IOTSensorDHT22:

    def __init__(self,
                 id=_id,
                 mqtt_server="mqtt",
                 mqtt_port=1883,
                 sensor_config_topic=None,
                 sensor_pin=4,
                 sensor_hum_topic=None,
                 sensor_temp_topic = None,
                 sensor_interval_topic = None,
                 sensor_interval=60):
        self.id = id
        self.messages = None
        self.values = None
        self.sensor_name_topic = None
        self.sensor_name = None
        self.mqtt_server = mqtt_server
        self.mqtt_port = mqtt_port
        self.sensor_pin = sensor_pin
        self.sensor_hum_topic = sensor_hum_topic
        self.sensor_temp_topic = sensor_temp_topic
        self.sensor_interval = sensor_interval
        self.sensor_interval_topic = sensor_interval_topic
        self.sensor_config_topic = sensor_config_topic
        self.time = None
        self.time_topic = None
        self.mqtt_client_id = f"mqtt_client_{_id}"

    async def query_configuration(self):
        if self.sensor_config_topic is not None:
            print("query config...")
            config = await self._query(self.sensor_config_topic, JSON)
            print(f"config: {config}")
            self.sensor_interval_topic = config["topic_interval"]
            self.sensor_name = config["name"]
            self.sensor_temp_topic = config["topic_temp"]
            self.sensor_hum_topic = config["topic_hum"]
            self.sensor_pin = int(config["sensor_pin"])
            self.time_topic = config["topic_time"]

    async def query_interval(self):
        if self.sensor_interval_topic is not None:
            print("query interval...")
            new_interval = await self._query(self.sensor_interval_topic, INT)
            if new_interval != self.sensor_interval:
                print(f"new interval {new_interval} - old: {self.sensor_interval} - assigning new value")
                self.sensor_interval = new_interval
                
    async def query_time(self):
        self.time = await self._query(self.time_topic, STRING)
        print(f"queried time: {self.time}")

    async def _query(self, topic, answer_type):
        if answer_type == JSON:
            return JSONDecoder().decode(bytes.decode(subscribe.simple(topic, hostname=self.mqtt_server).payload))
        elif answer_type == INT:
            return int(subscribe.simple(topic, hostname=self.mqtt_server).payload)
        elif answer_type == STRING:
            return str(bytes.decode(subscribe.simple(topic, hostname=self.mqtt_server).payload))
        else:
            raise TypeError
    

    async def publish_values(self):
        print(f"publishing messages: {self.messages}")
        publish.multiple(self.messages, hostname=self.mqtt_server)

    async def measure(self):
        values = Adafruit_DHT.read_retry(Adafruit_DHT.DHT22, self.sensor_pin)
        self.values = tuple([round(x, 1) for x in values])
        print(f"measured values: {self.values}")

    async def build_messages(self):
        self.messages = (
            (self.sensor_hum_topic, build_json({"hum": round(self.values[0], 1), "sensor_name": self.sensor_name, "time": self.time})),
            (self.sensor_temp_topic, build_json({"temp": round(self.values[1], 1), "sensor_name": self.sensor_name, "time": self.time}))
            )
        print(f"built messages: {self.messages}")

    def sleep(self):
        print(f"going to sleep for {self.sensor_interval}")
        sleep(self.sensor_interval)

    def run(self):
        print(f"Starting sensor {_id}")
        while True:
            asyncio.run(
                run_sequence(
                    self.query_configuration(),
                    run_parallel(
                        run_sequence(self.measure(), self.query_time(), self.build_messages(), self.publish_values()),
                        self.query_interval()),
                    )
            )
            self.sleep()
            


if __name__ == "__main__":
        IOTSensorDHT22(
            mqtt_server="192.168.188.37",  #
            sensor_config_topic = f"/sensor/config/{_id}").run()
