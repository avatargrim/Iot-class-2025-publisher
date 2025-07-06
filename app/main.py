import asyncio
import time
import json
import random
from aiomqtt import Client
import os
from datetime import datetime
import sys
import logging

# Fix Windows event loop policy (required for asyncio on Windows)
if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv(os.path.dirname(os.path.abspath(__file__)) + "/.env")

# Load MQTT configuration from environment variables
MQTT_BROKER = os.getenv("MQTT_GATEWAY", "localhost")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot-frames-model")
MQTT_PORT = os.getenv("MQTT_PORT", "1883")
MQTT_QOS = os.getenv("MQTT_QOS", "1")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Configure logging
log_level_str = LOG_LEVEL.upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Get sensor IDs from environment variable and generate sensor configs
sensor_ids_str = os.getenv("SENSOR_IDS", "0")
sensor_ids = [int(x.strip()) for x in sensor_ids_str.split(",") if x.strip().isdigit()]
sensor_configs = [
    {
        "id": f"{i:03}{i:03}{i:03}",
        "name": f"iot_sensor_{i}",
        "place_id": f"{i:03}{i:03}{i:03}"
    }
    for i in sensor_ids
]

# Logic to determine fan speed based on sensor values (rule-based)
def calculate_fan_speed(temp, humidity, pressure=None, luminosity=None):
    if temp > 30 or humidity > 80:
        return 3
    elif temp > 27 or humidity > 70:
        return 2
    elif temp > 24 or humidity > 60:
        return 1

    if pressure and pressure < 990:
        return 2  # Low pressure may indicate unstable weather

    return 0  # No need to turn on the fan

# Coroutine to publish sensor data repeatedly
async def publish_sensor(sensor_config):
    while True:
        try:
            logging.info(f"[{sensor_config['name']}] Connecting: {MQTT_BROKER}:{MQTT_PORT} ...")
            async with Client(MQTT_BROKER) as client:
                logging.info(f"[{sensor_config['name']}] Connected: {MQTT_BROKER}:{MQTT_PORT}")
                while True:
                    # Randomly generate sensor readings
                    temperature = round(random.uniform(18, 35), 2)
                    humidity = random.randint(30, 90)
                    pressure = random.randint(99990, 105000)

                    # Construct the message payload
                    sensor_data = {
                        "id": sensor_config["id"],
                        "name": sensor_config["name"],
                        "place_id": sensor_config["place_id"],
                        "payload": {
                            "temperature": temperature,
                            "humidity": humidity,
                            "pressure": pressure,
                            # Only include fan_speed if this is the ground-truth sensor
                            # Model will predict for others
                        }
                    }

                    # Add fan_speed only for iot_sensor_0 (ground-truth)
                    if sensor_config["name"] == "iot_sensor_0":
                        sensor_data["payload"]["fan_speed"] = calculate_fan_speed(temperature, humidity, pressure)

                    # Encode message and publish
                    message = json.dumps(sensor_data)
                    await client.publish(topic=MQTT_TOPIC, payload=message.encode(), qos=int(MQTT_QOS))

                    # Log message details
                    logging.info(f"GATEWAY: {MQTT_BROKER} PORT={MQTT_PORT} TOPIC={MQTT_TOPIC} QOS={MQTT_QOS}")
                    logging.info(f"PUBLISHED: sensor_name: {sensor_config['name']}")
                    logging.info(f"MESSAGE: {message}")
                    logging.info(f"TOTAL PACKAGE SIZE: topic={len(MQTT_TOPIC.encode())} + message={len(message.encode())} = {len(MQTT_TOPIC.encode()) + len(message.encode())} Bytes\n")

                    # Wait before next publish
                    await asyncio.sleep(5)

        except Exception as e:
            # Handle connection or publishing errors
            logging.error(f"[{sensor_config['name']}] Connection error: {e}")
            logging.error(f"[{sensor_config['name']}] Reconnecting in 5 seconds...\n")
            await asyncio.sleep(5)

# Run all sensor publishers concurrently
async def main():
    await asyncio.gather(*(publish_sensor(config) for config in sensor_configs))

# Start the asyncio application
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info(f"Shutdown requested. Exiting...")
