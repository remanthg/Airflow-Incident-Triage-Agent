import logging
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False)
def triage_missing_input():
    @task(retries=0)
    def read_upstream_partition():
        expected = Path("/opt/airflow/demo-input/dt=2025-01-01/part-000.parquet")
        try:
            expected.read_bytes()
        except FileNotFoundError as exc:
            logging.getLogger(__name__).error("FileNotFoundError: expected upstream input %s", expected)
            raise FileNotFoundError(f"expected upstream input {expected}") from exc

    read_upstream_partition()


triage_missing_input()
