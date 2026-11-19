import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx

from triage.airflow_logs import read_airflow_log
from triage.schemas import Attempt, Diagnosis

COMPOSE = Path(__file__).with_name("compose.yaml")
BASE = "http://127.0.0.1:8080"
DAG = "triage_missing_input"
TASK = "read_upstream_partition"
ADMIN = "triage_demo_admin"
READER = "triage_demo_reader"


def docker(*args, capture=False):
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), *args], check=True, text=True,
        stdout=subprocess.PIPE if capture else sys.stderr, timeout=180,
    ).stdout


def wait_for(check, description):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            result = check()
            if result is not None:
                return result
        except httpx.TransportError:
            pass
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {description}.")


def login(client: httpx.Client, username: str, password: str) -> str:
    def get_token():
        response = client.post("/auth/token", json={"username": username, "password": password})
        if response.is_server_error or response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["access_token"]

    return wait_for(get_token, "Airflow authentication")


def run_demo():
    if not shutil.which("docker"):
        raise RuntimeError("Docker with Linux container support and Compose is required for this Airflow demo.")
    docker("up", "-d")
    with httpx.Client(base_url=BASE, timeout=5, trust_env=False) as client:
        wait_for(lambda: client.get("/api/v2/monitor/health"), "Airflow API")
        passwords = json.loads(docker("exec", "-T", "airflow", "cat", "/opt/airflow/triage-passwords.json",
                                      capture=True))
        reader_token = login(client, READER, passwords[READER])
        client.headers.update({"Authorization": f"Bearer {reader_token}"})
        dag_path = f"/api/v2/dags/{DAG}"

        def read_resource(path):
            response = client.get(path)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()

        wait_for(lambda: read_resource(dag_path), "the parsed demo DAG")
        admin_token = login(client, ADMIN, passwords[ADMIN])
        started = client.post(f"{dag_path}/dagRuns", headers={"Authorization": f"Bearer {admin_token}"},
                              json={"dag_run_id": f"manual__triage_{uuid4().hex}", "logical_date": None})
        started.raise_for_status()
        run_id = started.json()["dag_run_id"]

        def failed_attempt():
            task = read_resource(f"{dag_path}/dagRuns/{run_id}/taskInstances/{TASK}")
            if task and task["state"] == "success":
                raise RuntimeError("The demo task succeeded. Its input is expected to be absent.")
            if task and task["state"] == "failed":
                return Attempt(dag_id=task["dag_id"], run_id=task["dag_run_id"], task_id=task["task_id"],
                               try_number=task["try_number"])
            return None

        attempt = wait_for(failed_attempt, "the failed task attempt")
        command = [sys.executable, "-m", "triage", "diagnose", "--provider", "stub", "--log-source", "airflow"]
        for field, value in attempt.model_dump().items():
            command.extend([f"--{field.replace('_', '-')}", str(value)])
        process = subprocess.run(command, text=True, capture_output=True, timeout=120,
                                 env={**os.environ, "AIRFLOW_BASE_URL": BASE, "AIRFLOW_API_TOKEN": reader_token})
        result = Diagnosis.model_validate_json(process.stdout)
        if process.returncode or result.category != "missing_upstream_data" or result.citation_status != "valid":
            raise RuntimeError(f"Triage did not produce a valid missing-input diagnosis ({result.status}).")
        api_log = read_airflow_log(client, BASE, attempt)
        cited = {row.number: row.text for row in api_log.lines if row.number in result.cited_log_lines}
        if (result.log_source != api_log.source or set(cited) != set(result.cited_log_lines)
                or not any("FileNotFoundError: expected upstream input" in line for line in cited.values())):
            raise RuntimeError("The citation did not match the missing-input record in the Airflow API response.")
        print(json.dumps({"diagnosis": result.model_dump(), "verified_airflow_log_lines": cited}, indent=2))


if __name__ == "__main__":
    try:
        run_demo()
    except (RuntimeError, OSError, ValueError, httpx.HTTPError, subprocess.SubprocessError) as exc:
        detail = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
        sys.exit(f"Airflow demo failed: {detail}")
