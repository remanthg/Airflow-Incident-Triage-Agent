import json
import os
from collections.abc import Mapping
from urllib.parse import quote, urlsplit

import httpx
from mcp.server.fastmcp.exceptions import ToolError

from .schemas import Attempt, NumberedLine, TaskLog

AIRFLOW_ENV = ("AIRFLOW_BASE_URL", "AIRFLOW_API_TOKEN")


def http_url(value: str, setting: str) -> str:
    """Permit TLS or plain HTTP on loopback and keep credentials out of URLs."""
    try:
        url = urlsplit(value)
        secure = url.scheme == "https" or (
            url.scheme == "http" and url.hostname in {"127.0.0.1", "localhost", "::1"}
        )
        if (secure and url.hostname and url.username is None and url.password is None
                and not url.query and not url.fragment and "\\" not in value
                and not any(c.isspace() for c in value)):
            return value
    except ValueError:
        pass
    raise ValueError(f"{setting} must be a credential-free HTTPS URL or loopback HTTP URL.")


def task_log_url(base_url: str, attempt: Attempt) -> str:
    identifiers = (attempt.dag_id, attempt.run_id, attempt.task_id)
    if any(part in {".", ".."} or "/" in part or "\\" in part for part in identifiers):
        raise ToolError("Airflow attempt identifiers must be single URL path segments.")
    dag, run, task = (quote(part, safe="") for part in identifiers)
    return (f"{base_url.rstrip('/')}/api/v2/dags/{dag}/dagRuns/{run}"
            f"/taskInstances/{task}/logs/{attempt.try_number}")


def read_airflow_log(client: httpx.Client, base_url: str, attempt: Attempt) -> TaskLog:
    url = task_log_url(base_url, attempt)
    params: dict[str, str | int] = {"full_content": "true", "map_index": -1}
    records: list[str] = []
    seen_tokens: set[str] = set()
    for _ in range(32):
        try:
            response = client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            detail = "task attempt was not found" if status == 404 else "read request was rejected"
            raise ToolError(f"Airflow log API HTTP {status}: {detail}.") from None
        except httpx.RequestError as exc:
            raise ToolError(f"Airflow log API request failed ({type(exc).__name__}).") from None

        try:
            data = response.json()
            content = data["content"]
            token = data.get("continuation_token")
            if not isinstance(content, list) or not all(isinstance(row, dict) for row in content):
                raise ValueError("Expected structured log records")
            if token is not None and not isinstance(token, str):
                raise ValueError("Expected a continuation token")
        except (ValueError, KeyError, TypeError):
            raise ToolError("Airflow log API returned an invalid log response.") from None

        records.extend(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in content)
        if not token:
            return TaskLog(source=url, lines=[
                NumberedLine(number=i, text=line) for i, line in enumerate(records, 1)
            ])
        if token in seen_tokens:
            break
        seen_tokens.add(token)
        params["token"] = token
    raise ToolError("Airflow log API did not finish paging the task attempt log.")


def airflow_log(attempt: Attempt, env: Mapping[str, str] = os.environ) -> TaskLog:
    base_url, token = (env.get(key) for key in AIRFLOW_ENV)
    if not base_url or not token:
        raise ToolError("Airflow log mode requires AIRFLOW_BASE_URL and AIRFLOW_API_TOKEN.")
    try:
        base_url = http_url(base_url, "AIRFLOW_BASE_URL")
    except ValueError as exc:
        raise ToolError(str(exc)) from None
    with httpx.Client(headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                      timeout=10, trust_env=False, follow_redirects=False) as client:
        return read_airflow_log(client, base_url, attempt)
