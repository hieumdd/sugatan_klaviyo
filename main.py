from models import Klaviyo
from tasks import create_task


def main(request):
    """API Gateway

    Args:
        request (flask.Request): HTTP request

    Raises:
        NotImplementedError: On no matching modules

    Returns:
        dict: HTTP Response
    """
        
    data = request.get_json()
    print(data)

    if "tasks" in data:
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
