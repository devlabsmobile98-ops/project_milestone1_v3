import os
import json
import time
import random
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("GCP_PROJECT")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "smart-readings")

if not PROJECT_ID:
    raise RuntimeError("Missing env var GCP_PROJECT (export GCP_PROJECT=your-project-id)")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

def make_reading(i: int) -> dict:
    # Every 5th message has a missing measurement to test FilterReading
    if i % 5 == 0:
        return {
            "device_id": f"D{i%3}",
            "timestamp": int(time.time()),
            "pressure_kpa": None,   # Will be dropped
            "temp_c": round(random.uniform(10, 30), 2),
        }

    return {
        "device_id": f"D{i%3}",
        "timestamp": int(time.time()),
        "pressure_kpa": round(random.uniform(180, 260), 2),
        "temp_c": round(random.uniform(10, 30), 2),
    }

def main():
    for i in range(30):
        payload = make_reading(i)
        data = json.dumps(payload).encode("utf-8")

        # Route into FilterReading using attributes
        future = publisher.publish(topic_path, data, function="raw")
        msg_id = future.result()

        print(f"Published msg_id={msg_id} payload={payload}")
        time.sleep(0.5)

if __name__ == "__main__":
    main()
