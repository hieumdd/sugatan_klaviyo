import os
import json
from datetime import datetime
from abc import abstractmethod, ABCMeta

import requests
from google.cloud import bigquery
import jinja2


BASE_URL = "https://a.klaviyo.com/api"
API_VER = "v1"

BQ_CLIENT = bigquery.Client()


class Klaviyo(metaclass=ABCMeta):
    def __init__(self, client_name, private_key):
        self.client_name = client_name
        self.private_key = private_key
        self.dataset = f"{client_name}_Klaviyo"

    @staticmethod
    def factory(
        client_name,
        private_key,
        mode,
        metric=None,
        measurement=None,
        start=None,
        end=None,
    ):
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

        if mode == "campaigns":
            return KlaviyoCampaigns(client_name, private_key)
        if mode == "metrics":
            with open(f"configs/{client_name}.json") as f:
                config = json.load(f)
            metric_mapper = config["metric_mapper"]
            args = (
                client_name,
                private_key,
                metric,
                metric_mapper[metric],
                measurement,
                start,
                end,
            )
            if metric in ["Unsubscribed", "Placed Order"]:
                return KlaviyoConversion(*args)
            elif metric in [
                "Received Email",
                "Opened Email",
                "Clicked Email",
            ]:
                return KlaviyoStandard(*args)
            else:
                raise NotImplementedError("Metric not found")

    def get(self):
        """Get the data from API

        Returns:
            dict: Responses from API
        """

        endpoint = self._get_endpoint()
        url = f"{BASE_URL}/{API_VER}/{endpoint}"
        return self._get(url)

    @abstractmethod
    def _get(self, url):
        pass

    @abstractmethod
    def _get_endpoint(self):
        pass

    def transform(self, rows):
        return self._transform(rows)

    @abstractmethod
    def _transform(self, rows):
        pass

    def load(self, rows):
        """Load to BigQuery

        Args:
            rows (list): List of rows in JSON

        Returns:
            google.cloud.bigquery._AsyncJob: Load Job
        """

        table = self._get_table()
        schema = self._get_schema()
        write_disposition = self._get_write_disposition()

        loads = BQ_CLIENT.load_table_from_json(
            rows,
            f"{self.dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
                schema=schema,
            ),
        ).result()
        return loads

    @abstractmethod
    def _get_table(self):
        pass

    @abstractmethod
    def _get_schema(self):
        pass

    @abstractmethod
    def _get_write_disposition(self):
        pass

    def update(self):
        return self._update()

    @abstractmethod
    def _update(self):
        pass

    def run(self):
        """Run the job

        Returns:
            dict: Job's Results
        """

        rows = self.get()
        responses = self._get_responses()
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

    @abstractmethod
    def _get_responses(self, responses):
        raise NotImplementedError


class KlaviyoMetric(Klaviyo):
    def __init__(
        self, client_name, private_key, metric, metric_id, measurement, start, end
    ):
        """Initiate Klaviyo Metric

        Args:
            metric (str): Metric Name
            metric_id (str): Metric ID/Statistic ID
            measurement (str): Corresponds to measurement for API
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d
        """

        super().__init__(client_name, private_key)
        self.metric = metric
        self.metric_id = metric_id
        self.measurement = measurement
        self.start = start
        self.end = end
        self.now = datetime.utcnow().isoformat(timespec="seconds")

    def _get(self, url):
        params = {
            "api_key": self.private_key,
            "unit": "day",
            "measurement": self.measurement,
            "by": self._params_by(),
            "count": 10000,
        }
        if self.start and self.end:
            params["start_date"] = self.start
            params["end_date"] = self.end

        with requests.get(url, params=params) as r:
            res = r.json()
        return res

    def _get_endpoint(self):
        return f"metric/{self.metric_id}/export"

    @abstractmethod
    def _params_by(self):
        """Abstract Method to get the "by" params for API

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError

    def _transform(self, results):
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

    def _get_table(self):
        return f"_stage_{self.table}"

    def _get_schema(self):
        with open("schemas/metrics.json", "r") as f:
            return json.load(f)

    def _get_write_disposition(self):
        return "WRITE_APPEND"

    def _update(self):
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

    def _get_responses(self):
        return {
            "metric": self.metric,
            "start_date": getattr(self, "start", None),
            "end_date": getattr(self, "end", None),
        }


class KlaviyoStandard(KlaviyoMetric):
    def __init__(
        self, client_name, private_key, metric, metric_id, measurement, start, end
    ):
        super().__init__(
            client_name, private_key, metric, metric_id, measurement, start, end
        )

    def _params_by(self):
        return "$message"


class KlaviyoConversion(KlaviyoMetric):
    def __init__(
        self, client_name, private_key, metric, metric_id, measurement, start, end
    ):
        super().__init__(
            client_name, private_key, metric, metric_id, measurement, start, end
        )

    def _params_by(self):
        return "$attributed_message"


class KlaviyoCampaigns(Klaviyo):
    def __init__(self, client_name, private_key):
        """Initiate Klaviyo Campaigns Job"""

        super().__init__(client_name, private_key)
        self.table = "_Campaigns"

    def _get(self, url):
        rows = []
        with requests.Session() as session:
            while True:
                params = {"api_key": self.private_key, "count": 100, "page": 0}
                with session.get(url, params=params) as r:
                    res = r.json()
                if len(res["data"]) > 0:
                    rows.extend(res["data"])
                    params["page"] += 1
                else:
                    break
        return rows

    def _get_endpoint(self):
        return "campaigns"

    def _transform(self, rows):
        return rows

    def _get_table(self):
        return self.table

    def _get_schema(self):
        with open("schemas/campaigns.json", "r") as f:
            return json.load(f)

    def _get_write_disposition(self):
        return "WRITE_TRUNCATE"

    def _update(self):
        pass

    def _get_responses(self):
        return {
            "table": self.table,
        }
