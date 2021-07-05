import os
import json
from datetime import datetime
from abc import abstractmethod, ABCMeta

import requests
from google.cloud import bigquery
import jinja2


# CLIENT_NAME = os.getenv("CLIENT_NAME")
BASE_URL = "https://a.klaviyo.com/api"
API_VER = "v1"

BQ_CLIENT = bigquery.Client()
# DATASET = f"{CLIENT_NAME}_Klaviyo"

class Klaviyo:
    def __init__(self, client_name, private_token):
        self.client_name = client_name
        self.private_token = private_token
        self.dataset = f"{client_name}_Klaviyo"

    @staticmethod
    def create(client_name, private_token, mode, metric=None, measurement=None, start=None, end=None):
        """Factory method to create Klaviyo Job

        Args:
            metric (str): Metric Name
            measurement (str): Corresponding to measurement for API
            start (str, optional): Date in %Y-%m-%d. Defaults to None.
            end (str, optional): Date in %Y-%m-%d. Defaults to None.

        Raises:
            NotImplementedError: When no metric found

        Returns:
            KlaviyoMetric: Klaviyo Metric Job
        """
        if mode == 'campaigns':
            return KlaviyoCampaigns(client_name, private_token)
        if mode == 'metrics':
            with open(f"configs/{client_name}.json") as f:
                config = json.load(f)
            metric_mapper = config["metric_mapper"]
            if metric == "Placed Order":
                return KlaviyoConversion(
                    client_name,
                    private_token,
                    metric,
                    metric_mapper[metric],
                    measurement,
                    start,
                    end,
                )
            elif metric in [
                "Received Email",
                "Opened Email",
                "Clicked Email",
                "Unsubscribed",
            ]:
                return KlaviyoStandard(
                    client_name,
                    private_token,
                    metric,
                    metric_mapper[metric],
                    measurement,
                    start,
                    end,
                )
            else:
                raise NotImplementedError("Metric not found")


class KlaviyoMetric(metaclass=ABCMeta):
    def __init__(
        self, client_name, private_token, metric, metric_id, measurement, start, end
    ):
        """Initiate Klaviyo Metric

        Args:
            metric (str): Metric Name
            metric_id (str): Metric ID/Statistic ID
            measurement (str): Corresponds to measurement for API
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d
        """
        self.client_name = client_name
        self.private_token = private_token
        self.metric = metric
        self.metric_id = metric_id
        self.measurement = measurement
        self.start = start
        self.end = end
        self.now = datetime.utcnow().isoformat(timespec="seconds")
        self.dataset = f"{client_name}_Klaviyo"

    @staticmethod
    def create(client_name, private_token, metric, measurement, start=None, end=None):
        """Factory method to create Klaviyo Metric Job

        Args:
            metric (str): Metric Name
            measurement (str): Corresponding to measurement for API
            start (str, optional): Date in %Y-%m-%d. Defaults to None.
            end (str, optional): Date in %Y-%m-%d. Defaults to None.

        Raises:
            NotImplementedError: When no metric found

        Returns:
            KlaviyoMetric: Klaviyo Metric Job
        """

        start = start
        end = end
        with open(f"configs/{client_name}.json") as f:
            config = json.load(f)
        metric_mapper = config["metric_mapper"]
        if metric == "Placed Order":
            return KlaviyoConversion(
                client_name,
                private_token,
                metric,
                metric_mapper[metric],
                measurement,
                start,
                end,
            )
        elif metric in [
            "Received Email",
            "Opened Email",
            "Clicked Email",
            "Unsubscribed",
        ]:
            return KlaviyoStandard(
                client_name,
                private_token,
                metric,
                metric_mapper[metric],
                measurement,
                start,
                end,
            )
        else:
            raise NotImplementedError("Metric not found")

    def get(self):
        """Get the data from API

        Returns:
            dict: Responses from API
        """

        endpoint = f"metric/{self.metric_id}/export"
        url = f"{BASE_URL}/{API_VER}/{endpoint}"

        params = {
            "api_key": self.private_token,
            "unit": "day",
            "measurement": self.measurement,
            "by": self._params_by(),
            "count": 10000,
        }
        if self.start and self.end:
            params["start_date"] = self.start
            params["end_date"] = self.end

        with requests.Session() as session:
            with session.get(url, params=params) as r:
                res = r.json()
        return res

    @abstractmethod
    def _params_by(self):
        """Abstract Method to get the "by" params for API

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError

    def transform(self, results):
        """Parse & transform responses from API

        Args:
            results (dict): Reponses from API

        Returns:
            list: List of rows in JSON
        """

        self.table = results["metric"]["name"].replace(" ", "")
        segments = results["results"]
        rows = [self._parse_segment(segment) for segment in segments]
        rows = [item for sublist in rows for item in sublist]
        rows = [
            {
                "metric": results["metric"]["name"],
                "metric_id": results["metric"]["id"],
                "measurement": self.measurement,
                **row,
                "_batched_at": self.now,
            }
            for row in rows
        ]
        return rows

    def _parse_segment(self, segment):
        """Parse each rows within segments

        Args:
            segment (dict): Segments from API responses

        Returns:
            list: List of rows in JSON
        """

        attributed_message = segment["segment"]
        rows = [
            {
                "attributed_message": attributed_message,
                "date": _data["date"],
                "values": _data["values"][0],
            }
            for _data in segment["data"]
        ]
        return rows

    def load(self, rows):
        """Load rows to staging table on BigQuery

        Args:
            rows (list): List of row in JSON

        Returns:
            google.cloud.bigquery._AsyncJob: Load job
        """

        with open("schemas/metrics.json", "r") as f:
            schema = json.load(f)

        loads = BQ_CLIENT.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=schema,
            ),
        ).result()
        return loads

    def update(self):
        """Update the main table using the staging table"""

        loader = jinja2.FileSystemLoader(searchpath="./templates")
        env = jinja2.Environment(loader=loader)

        template = env.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=self.dataset,
            table=self.table,
            p_key=",".join(
                ["date", "metric", "metric_id", "attributed_message", "measurement"]
            ),
            incremental_key="_batched_at",
        )
        BQ_CLIENT.query(rendered_query)

    def run(self):
        """Run the job

        Returns:
            dict: Job's Results
        """

        rows = self.get()
        responses = {
            "metric": self.metric,
            "start_date": getattr(self, "start", None),
            "end_date": getattr(self, "end", None),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            _ = self.update()
            responses = {
                **responses,
                "num_processed": len(rows),
                "output_rows": loads.output_rows,
                "errors": loads.errors,
            }
        return responses


class KlaviyoStandard(KlaviyoMetric):
    def __init__(
        self, client_name, private_token, metric, metric_id, measurement, start, end
    ):
        super().__init__(
            client_name, private_token, metric, metric_id, measurement, start, end
        )

    def _params_by(self):
        return "$message"


class KlaviyoConversion(KlaviyoMetric):
    def __init__(
        self, client_name, private_token, metric, metric_id, measurement, start, end
    ):
        super().__init__(
            client_name, private_token, metric, metric_id, measurement, start, end
        )

    def _params_by(self):
        return "$attributed_message"


class KlaviyoCampaigns:
    def __init__(self):
        """Initiate Klaviyo Campaigns Job"""

        self.table = "_Campaigns"

    def get(self):
        """Get data from API

        Returns:
            list: List of row in JSON
        """

        endpoint = "campaigns"
        url = f"{BASE_URL}/{API_VER}/{endpoint}"

        i = 0
        rows = []
        with requests.Session() as session:
            while True:
                params = {"api_key": self.private_token, "count": 100, "page": i}
                with session.get(url, params=params) as r:
                    res = r.json()
                if len(res["data"]) > 0:
                    rows.extend(res["data"])
                    i = i + 1
                else:
                    break
        return rows

    def load(self, rows):
        """Load to BigQuery

        Args:
            rows (list): List of rows in JSON

        Returns:
            google.cloud.bigquery._AsyncJob: Load Job
        """

        with open("schemas/campaigns.json", "r") as f:
            schema = json.load(f)

        loads = BQ_CLIENT.load_table_from_json(
            rows,
            f"{self.dataset}.{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                schema=schema,
            ),
        ).result()
        return loads

    def run(self):
        """Run the job

        Returns:
            dict: Job's Results
        """

        rows = self.get()
        responses = {
            "table": self.table,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            loads = self.load(rows)
            responses = {
                **responses,
                "output_rows": loads.output_rows,
            }
        return responses
