import os
import json
import uuid

from google.cloud import secretmanager, tasks_v2

SECRET_CLIENT = secretmanager.SecretManagerServiceClient()
TASKS_CLIENT = tasks_v2.CloudTasksClient()
CLOUD_TASKS_PATH = (
    os.getenv("PROJECT_ID"),
    os.getenv("REGION"),
    os.getenv("QUEUE_ID"),
)
PARENT = TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH)


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
    clients_keys
    return clients_keys


def create_task(tasks_data):
    clients = get_clients()
    payloads = [
        {
            "tasks": tasks_data["tasks"],
            **client,
        }
        for client in clients
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(
                *CLOUD_TASKS_PATH,
                task=f"{payload['client_name']}-{uuid.uuid4()}",
            ),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"https://{os.getenv('REGION')}-{os.getenv('PROJECT_ID')}.cloudfunctions.net/{os.getenv('FUNCTION_NAME')}",
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": PARENT,
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "tasks": len(responses),
        "data": tasks_data,
    }
