from google.cloud import pubsub_v1   # pip install google-cloud-pubsub
import glob
import os
import csv
import json
import time

project_id = "project-id-is-hidden"
topic_name = "labelsTopic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing CSV records to {topic_path}")

ID_COUNTER = 1

with open("Labels.csv", "r", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:
        # Convert empty CSV fields "" -> None (shows as null in JSON)
        for k in row:
            if row[k] == "":
                row[k] = None

        # Ensure keys are numerically converted for rows in MySQL Table
        row["ID"] = ID_COUNTER
        ID_COUNTER += 1

        # Ensure time is int
        if row.get("time") is not None:
            row["time"] = int(float(row["time"]))

        # Convert numeric fields to float 
        for k in ["temperature", "humidity", "pressure"]:
            if row.get(k) is not None:
                row[k] = float(row[k])

        record_value = json.dumps(row).encode("utf-8") #serialize this message

        try:
            future = publisher.publish(topic_path, record_value)
            # ensure that publishing has been completed successfully
            future.result()
            print("Published:", row)
        except Exception as e:
            print("Failed to publish:", e)

        time.sleep(0.2)

