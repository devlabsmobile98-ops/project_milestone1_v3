import os, json
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("GCP_PROJECT")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "smart-readings")
SUB_ID     = os.environ.get("SUB_ID", "smart-convert-sub")

subscriber = pubsub_v1.SubscriberClient()
publisher  = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
sub_path   = subscriber.subscription_path(PROJECT_ID, SUB_ID)

FILTER = 'attributes.function="filtered"'

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        payload = json.loads(message.data.decode("utf-8"))
    except Exception:
        message.ack()
        return

    # conversions
    kpa = float(payload["pressure_kpa"])
    c   = float(payload["temp_c"])

    payload["pressure_psi"] = kpa / 6.895
    payload["temp_f"] = c * 1.8 + 32

    attrs = dict(message.attributes)
    attrs["function"] = "converted"

    publisher.publish(topic_path, json.dumps(payload).encode("utf-8"), **attrs)
    message.ack()

def ensure_subscription():
    try:
        subscriber.create_subscription(name=sub_path, topic=topic_path, filter=FILTER)
    except Exception:
        pass

def main():
    ensure_subscription()
    streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
    print("ConvertReading running...")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    if not PROJECT_ID:
        raise RuntimeError("Missing env GCP_PROJECT")
    main()
