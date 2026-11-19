import asyncio
import json
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import httpx
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from triage.airflow_logs import read_airflow_log
from triage.log_client import LogToolError, fetch_task_log
from triage.models import stub_model
from triage.schemas import Attempt
from triage.workflow import build_graph

ATTEMPT = Attempt(dag_id="triage_missing_input", run_id="manual__2025-01-01T02:00:00+00:00",
                  task_id="read_upstream_partition", try_number=2)
LOG_PATH = ("/api/v2/dags/triage_missing_input/dagRuns/manual__2025-01-01T02%3A00%3A00%2B00%3A00"
            "/taskInstances/read_upstream_partition/logs/2")
CAUSE = "FileNotFoundError: expected upstream input /data/dt=2025-01-01/part-000.parquet"


def test_airflow_attempt_log_over_mcp_stdio(monkeypatch):
    token = uuid4().hex
    requests = []

    class LogAPI(BaseHTTPRequestHandler):
        def do_GET(self):
            target = urlsplit(self.path)
            query = parse_qs(target.query)
            requests.append(query)
            if self.headers.get("Authorization") != f"Bearer {token}":
                self.send_error(401)
                return
            if target.path != LOG_PATH:
                self.send_error(404)
                return
            if query.get("token") == ["next"]:
                payload = {"content": [{"event": CAUSE, "level": "error"}], "continuation_token": None}
            else:
                payload = {"content": [{"event": "Reading parquet partition"}], "continuation_token": "next"}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), LogAPI) as api:
        base = f"http://127.0.0.1:{api.server_port}"
        monkeypatch.setenv("AIRFLOW_BASE_URL", base)
        monkeypatch.setenv("AIRFLOW_API_TOKEN", token)
        thread = threading.Thread(target=api.serve_forever, daemon=True)
        thread.start()
        try:
            reader = partial(fetch_task_log, source="airflow")
            outcome = asyncio.run(build_graph(stub_model, reader).ainvoke({"attempt": ATTEMPT}))
            result = outcome["diagnosis"]
            assert result.category == "missing_upstream_data"
            assert result.citation_status == "valid"
            assert result.log_source == base + LOG_PATH
            assert result.cited_log_lines == [2]
            assert json.loads(outcome["task_log"].lines[1].text)["event"] == CAUSE
            assert requests[:2] == [
                {"full_content": ["true"], "map_index": ["-1"]},
                {"full_content": ["true"], "map_index": ["-1"], "token": ["next"]},
            ]
            with pytest.raises(LogToolError, match="HTTP 404.*attempt was not found"):
                asyncio.run(reader(ATTEMPT.model_copy(update={"try_number": 1})))
        finally:
            api.shutdown()
            thread.join()


def test_airflow_rejects_repeated_continuation_token():
    transport = httpx.MockTransport(lambda _: httpx.Response(
        200, json={"content": [], "continuation_token": "stuck"}
    ))
    with httpx.Client(transport=transport) as client:
        with pytest.raises(ToolError, match="did not finish paging"):
            read_airflow_log(client, "http://127.0.0.1:8080", ATTEMPT)
