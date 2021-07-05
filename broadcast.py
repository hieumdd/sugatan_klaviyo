import os
import json
from google.cloud import pubsub_v1


def get_clients():
    clients = [i.replace(".json", "") for i in os.listdir("configs")]
    clients = [
        {"client_name": i, "private_key": os.getenv(f"{i}_PRIVATE_KEY")}
        for i in clients
    ]
    return clients


def broadcast(broadcast_data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))
    clients = get_clients()

    for client in clients():
        data = {
            "client_name": client["client_name"],
            "private_key": client["private_key"],
            "mode": broadcast_data["mode"],
            "start": broadcast_data.get("start"),
            "end": broadcast_data.get("end"),
        }
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return len(clients)
