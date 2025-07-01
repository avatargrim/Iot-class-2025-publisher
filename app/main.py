import asyncio
import time
import json
import random
from aiomqtt import Client
import os
from datetime import datetime
import sys
import logging

if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

# Load environment variables (useful when working locally)
from dotenv import load_dotenv
load_dotenv(os.path.dirname(os.path.abspath(__file__))+"/.env")

# get MQTT configure
MQTT_BROKER = os.getenv("MQTT_GATEWAY", "localhost") # user MQTT_GATEWAY to publish as sensors
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot-frames-model")
MQTT_PORT = os.getenv("MQTT_PORT", "1883")
MQTT_QOS = os.getenv("MQTT_QOS", "1")
MQTT_QOS = os.getenv("MQTT_QOS", "1")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Logggin env
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

sensor_configs = [
    {
        "id": f"{i:03}{i:03}{i:03}",
        "name": f"iot_sensor_{i}",
        "place_id": f"{i:03}{i:03}{i:03}"
    }
    for i in range(0, 1)
]

# สามารถปรับให้ใช้ร่วมกับ CO₂, PM2.5, AQI, หรือ luminosity ได้ในอนาคต
def calculate_fan_speed(temp, humidity, pressure=None, luminosity=None):
    # ความสำคัญหลัก: อุณหภูมิและความชื้น
    if temp > 30 or humidity > 80:
        return 3
    elif temp > 27 or humidity > 70:
        return 2
    elif temp > 24 or humidity > 60:
        return 1

    # กรณีสภาพอื่น ๆ แม้ temp/humidity ต่ำ แต่ pressure/luminosity สูง
    if pressure and pressure < 990:
        return 2  # แรงดันต่ำ = อากาศแปรปรวน อาจเร่งระบาย

    return 0  # ปกติ ไม่ร้อน ไม่ชื้น ไม่ต้องเปิด
    
async def publish_sensor(sensor_config):
    while True:
        try:
            logging.info(f"[{sensor_config['name']}] Connecting: {MQTT_BROKER}:{MQTT_PORT} ...")
            async with Client(MQTT_BROKER) as client:
                logging.info(f"[{sensor_config['name']}] Connected: {MQTT_BROKER}:{MQTT_PORT}")
                while True:
                    temperature = round(random.uniform(18, 35), 2)
                    humidity = random.randint(30, 90)
                    pressure = random.randint(980, 1050)
                    # luminosity = random.randint(100, 100000)
                    sensor_data = {
                        "id": sensor_config["id"],
                        "name": sensor_config["name"],
                        "place_id": sensor_config["place_id"],
                        # Update timestamp and formatted date each time
                        # "timestamp" : int(time.time() * 1000),  # Current time in milliseconds
                        # "date" : time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),  # UTC date-time
                        "payload": {
                            "temperature": temperature,
                            "humidity": humidity,
                            "pressure": pressure,
                            "fan_speed": calculate_fan_speed(temperature, humidity, pressure)  # 0=off, 1=low, 2=medium, 3=high
                        }
                    }

                    message = json.dumps(sensor_data)
                    await client.publish(topic=MQTT_TOPIC, payload=message.encode(), qos=int(MQTT_QOS))
                    logging.info(f"GATEWAY: {MQTT_BROKER} PORT={MQTT_PORT} TOPIC={MQTT_TOPIC} QOS={MQTT_QOS}")
                    logging.info(f"PUBLISHED: sensor_name: {sensor_config['name']}")
                    logging.info(f"MESSAGE: {message}")
                    logging.info(f"TOTAL PACKAGE SIZE: topic={len(MQTT_TOPIC.encode())} + message={len(message.encode())} = {len(MQTT_TOPIC.encode()) + len(message.encode())} Bytes\n")

                    # await asyncio.sleep(random.uniform(1, 2))
                    await asyncio.sleep(5)

        except Exception as e:
            logging.error(f"[[{sensor_config['name']}] Connection error: {e}")
            logging.error(f"[[{sensor_config['name']}] Reconnecting in 5 seconds...\n")
            await asyncio.sleep(5)


async def main():
    await asyncio.gather(*(publish_sensor(config) for config in sensor_configs))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info(f"Shutdown requested. Exiting...")
