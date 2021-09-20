from models import Klaviyo
from tasks import create_task


def main(request):
    data = request.get_json()
    print(data)

    if "mode" and "client_name" not in data:
        response = create_task(data)
    elif "client_name" in data:
        job = Klaviyo.factory(
            data["mode"],
            data["client_name"],
            data["private_key"],
            data.get("start"),
            data.get("end"),
        )
        response = job.run()
    else:
        raise NotImplementedError(data)

    print(response)
    return response
