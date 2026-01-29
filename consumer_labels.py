from google.cloud import pubsub_v1 # pip install google-cloud-pubsub
import glob                      # for searching for json file
import json
import os

# Set the project_id with your project ID
project_id = "project-id-is_hidden";
subscription_id = "labelsTopic-sub";  # default subscription created automatically

# create a subscriber and get the subscription path
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
print("Listening for messages on {}".format(subscription_path))

def callback(message):
    try:
        record = json.loads(message.data.decode('utf-8'));  # deserialize JSON -> dictionary

        print("\nReceived record:")
        print("time:", record.get("time"))
        print("profileName:", record.get("profileName"))
        print("temperature:", record.get("temperature"))
        print("humidity:", record.get("humidity"))
        print("pressure:", record.get("pressure"))

        message.ack();  # acknowledge

    except Exception as e:
        print("Failed to process message:", e)

subscriber.subscribe(subscription_path, callback=callback)
print("Consumer is running!")

while(True):
    pass
