from time import sleep
import asyncio
from paho.mqtt import subscribe, publish
from random import randint
from typing import Awaitable, Any


async def run_sequence(*functions: Awaitable[Any]) -> None:
    for function in functions:
        await function


async def run_parallel(*functions: Awaitable[Any]) -> None:
    await asyncio.gather(*functions)


class IOTSensorDHT22:

    def __init__(self,
                 mqtt_server="localhost",
                 mqtt_port=1883,
                 sensor_pin=4,
                 sensor_values_topic_prefix="/sensor/raw/bedroom/",
                 sensor_interval=60,
                 sensor_interval_topic="/setting/sensor/inside/power/interval"):
        self.messages = None
        self.values = None
        self.mqtt_server = mqtt_server
        self.mqtt_port = mqtt_port
        self.sensor_pin = sensor_pin
        self.sensor_values_topic_prefix = sensor_values_topic_prefix
        self.sensor_interval = sensor_interval
        self.sensor_interval_topic = sensor_interval_topic
        self.mqtt_client_id = f"mqtt_client_{randint(0, 1000)}"

    async def query_interval(self):
        new_interval = int(subscribe.simple(self.sensor_interval_topic, hostname=self.mqtt_server).payload)
        if new_interval != self.sensor_interval:
            print(
                f"new interval {new_interval} - old: {self.sensor_interval} - assigning new value: {new_interval != self.sensor_interval}")
            self.sensor_interval = new_interval

    async def publish_values(self):
        print(f"publishing messages: {self.messages}")
        publish.multiple(self.messages, hostname=self.mqtt_server)

    async def measure(self):
        self.values = Adafruit_DHT.read_retry(Adafruit_DHT.DHT22, self.sensor_pin)

    async def build_messages(self):
        messages = (self.build_message("hum", self.values[0]), self.build_message("temp", self.values[1]))
        print(messages)
        self.messages = messages

    def build_message(self, topic_appendix, value):
        return self.sensor_values_topic_prefix + topic_appendix, round(value, 1)

    def sleep(self):
        print(f"going to sleep for {self.sensor_interval}")
        sleep(self.sensor_interval)

    async def run(self):
        while True:
            await run_parallel(
                run_sequence(self.measure(), self.build_messages(), self.publish_values()),
                self.query_interval()
            )
            self.sleep()


if __name__ is "__main__":
    asyncio.run(
        IOTSensorDHT22(
            mqtt_server="192.168.188.37",  #
            sensor_pin=12,
            sensor_values_topic_prefix="/sensor/raw/bedroom/",
            sensor_interval=60,
            sensor_interval_topic="/setting/sensor/inside/power/interval/").run())
