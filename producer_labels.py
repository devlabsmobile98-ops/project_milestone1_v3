from google.cloud import pubsub_v1      # pip install google-cloud-pubsub
import glob                             # for searching for json file
import json
import os
import csv
import time

# Set the project_id with your project ID
project_id = "project-id-is-hidden";            
topic_name = "labelsTopic"; 

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print("Publishing CSV records to {}".format(topic_path))

# Read the CSV and publish each record
with open("Labels.csv", "r", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:
        # Convert empty CSV fields "" -> None (shows as null in JSON)
        for k in row:
            if(row[k] == ""):
                row[k] = None;

        record_value = json.dumps(row).encode('utf-8');   # serialize the message

        try:
            future = publisher.publish(topic_path, record_value);

            # ensure that the publishing has been completed successfully
            future.result()
            print("The message {} has been published successfully".format(row))

        except:
            print("Failed to publish the message")

        time.sleep(.2)

