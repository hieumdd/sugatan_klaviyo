import os
import json
from google.cloud import pubsub_v1, secretmanager

SECRET_CLIENT = secretmanager.SecretManagerServiceClient()


def get_clients():
    with open("configs/clients.json", "r") as f:
        clients = [
            {k: v["secret"]}
            for client in json.load(f)["clients"]
            for k, v in client.items()
        ]
    clients_keys = [
        {
            "client_name": k,
            "private_key": SECRET_CLIENT.access_secret_version(
                request={
                    "name": f"projects/{os.getenv('PROJECT_ID')}/secrets/{v['id']}/versions/{v['version']}",
                }
            ).payload.data.decode("UTF-8"),
        }
        for client in clients
        for k, v in client.items()
    ]
    return clients_keys


def broadcast(broadcast_data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))
    clients = get_clients()

    for client in clients:
        data = {
            "client_name": client["client_name"],
            "private_key": client["private_key"],
            "mode": broadcast_data["broadcast"],
            "start": broadcast_data.get("start"),
            "end": broadcast_data.get("end"),
        }
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return len(clients)
