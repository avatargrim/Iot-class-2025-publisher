import asyncio
import json
import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from aiomqtt import Client

# Windows asyncio policy (needed for some Python builds on Windows)
if sys.platform.lower().startswith("win") or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

# -------- Load .env --------
# Try to load both: project root `.env` and app local `.env`
from dotenv import load_dotenv

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(APP_DIR, ".."))
for env_path in (os.path.join(ROOT_DIR, ".env"), os.path.join(APP_DIR, ".env")):
    if os.path.exists(env_path):
        load_dotenv(env_path, override=False)

# -------- Configs --------
MQTT_BROKER = os.getenv("MQTT_GATEWAY") or os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot-frames-model")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

CSV_PATH = os.getenv("CSV_PATH", os.path.join(APP_DIR, "Air_Quality.csv"))
# Publish interval (seconds) between rows
PUBLISH_DELAY = float(os.getenv("PUBLISH_DELAY", "5"))
# once: publish each row exactly once; loop: restart at top after reaching the end
PUBLISH_MODE = os.getenv("PUBLISH_MODE", "loop").lower()  # "once" or "loop"

# Optional static identity fields if CSV does not include them
SENSOR_ID = os.getenv("SENSOR_ID", "000000")
SENSOR_NAME = os.getenv("SENSOR_NAME", "iot_sensor_csv")
PLACE_ID = os.getenv("PLACE_ID", "000000")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# -------- Helpers --------
def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Convert a DataFrame into a list of payload dicts expected by downstream consumers.
    If the CSV already contains id/name/place_id columns, use them; otherwise inject static ones.
    Recognizes a variety of timestamp column names and converts to ISO 8601.
    """
    # Try to detect a timestamp-like column
    ts_candidates = [
        "timestamp", "time", "datetime", "date", "DeviceTimeStamp",
        "Timestamp", "Time", "DateTime", "Date"
    ]
    ts_col = next((c for c in ts_candidates if c in df.columns), None)

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        # Prepare identity
        rid = str(row.get("id", row.get("sensor_id", SENSOR_ID)))
        rname = str(row.get("name", row.get("sensor_name", SENSOR_NAME)))
        rplace = str(row.get("place_id", PLACE_ID))

        # Build payload: include every numeric or string column except the identity fields
        payload = {}
        for col, val in row.items():
            if col in {"id", "sensor_id", "name", "sensor_name", "place_id"}:
                continue
            # Convert pandas NaN to None
            if pd.isna(val):
                payload[col] = None
            else:
                # Timestamps normalization
                if ts_col and col == ts_col:
                    try:
                        payload[col] = pd.to_datetime(val).isoformat()
                    except Exception:
                        payload[col] = str(val)
                else:
                    payload[col] = val

        # If there was no explicit timestamp column, add one
        if ts_col is None:
            payload["timestamp"] = datetime.utcnow().isoformat()

        record = {
            "id": rid,
            "name": rname,
            "place_id": rplace,
            "payload": payload,
        }
        records.append(record)
    return records


async def publish_from_csv(records: List[Dict[str, Any]]):
    """
    Connect once and stream each record as a JSON message.
    """
    logging.info(f"Connecting to MQTT {MQTT_BROKER}:{MQTT_PORT} topic={MQTT_TOPIC} qos={MQTT_QOS}")
    async with Client(MQTT_BROKER, port=MQTT_PORT) as client:
        logging.info("Connected to MQTT broker.")
        index = 0
        total = len(records)

        while True:
            if index >= total:
                if PUBLISH_MODE == "loop":
                    index = 0
                else:
                    logging.info("Finished publishing all CSV rows (mode=once).")
                    break

            msg_obj = records[index]
            msg_str = json.dumps(msg_obj, ensure_ascii=False)
            await client.publish(topic=MQTT_TOPIC, payload=msg_str.encode("utf-8"), qos=MQTT_QOS)

            logging.info(
                f"PUBLISHED row={index+1}/{total} bytes={len(msg_str.encode('utf-8'))} "
                f"topic={MQTT_TOPIC} qos={MQTT_QOS}"
            )
            logging.debug(f"MESSAGE: {msg_str}")

            index += 1
            await asyncio.sleep(PUBLISH_DELAY)


async def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    logging.info(f"Loading CSV: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    if df.empty:
        logging.warning("CSV is empty. Nothing to publish.")
        return

    records = df_to_records(df)
    logging.info(f"Prepared {len(records)} records from CSV.")
    await publish_from_csv(records)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown requested. Exiting...")
