# Airflow Incident Triage Agent

The CLI diagnoses a failed Airflow task attempt from a synthetic fixture or a local
Airflow instance. A LangGraph workflow calls the read-only `get_task_log` tool over
MCP stdio or OAuth-protected Streamable HTTP, retrieves up to three Markdown sections
with LangChain TF-IDF, and validates citations against the evidence passed to the model.
The CLI provides one read-only operator check for a diagnosis or abstention.

## Setup

Use Python 3.11 or newer. From the repository root, run the following in PowerShell.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest -q
```

On macOS or Linux, use `.venv/bin/python` in place of `.\.venv\Scripts\python.exe`.

## Offline demo

```powershell
.\.venv\Scripts\python.exe -m triage demo
```

The demo uses the deterministic stub and prints a JSON array with a missing upstream
input diagnosis and an abstention for a log without a cause. Both cases go through
the MCP subprocess and LangGraph. The second case abstains before model invocation.
The stub handles a few common signatures from the four development fixtures. It
requires zero credentials.

Use `diagnose` to look up a specific attempt.

```powershell
.\.venv\Scripts\python.exe -m triage diagnose --provider stub --dag-id daily_sales --run-id 'manual__2025-01-11T06:00:00+00:00' --task-id load_daily_sales --try-number 1
```

The `log_source` path is relative to this repository or to the installed package's
parent directory. Log citations are 1-based physical lines. The development example
above points to line 4 of [dev-01.log](triage/data/fixtures/logs/dev-01.log) and
`RB-101` in the [upstream data runbook](triage/data/runbooks/upstream-data.md).
The [fixture index](triage/data/fixtures/index.json) maps the four attempt identifiers
to each log. The other development logs cover timeout, import failure, and sparse evidence.

The first result from the offline demo is shown below.

```json
{
  "category": "missing_upstream_data",
  "explanation": "The task could not read an expected upstream input.",
  "cited_log_lines": [4],
  "cited_runbook_sections": ["RB-101"],
  "dag_id": "daily_sales",
  "run_id": "manual__2025-01-11T06:00:00+00:00",
  "task_id": "load_daily_sales",
  "try_number": 1,
  "next_check": "Inspect the upstream producer attempt for the expected data interval and output path.",
  "status": "diagnosed",
  "citation_status": "valid",
  "log_source": "triage/data/fixtures/logs/dev-01.log"
}
```

`status` distinguishes `diagnosed`, `abstained`, `tool_error`, and `model_error`.
Insufficient evidence and rejected citations both yield `unknown` with a reason.
Rejected citations are removed and marked `invalid`. Tool and model failures return
exit code 2. A completed run with an abstention returns exit code 0.

## Held-out evaluation

The OpenAI provider uses `OPENAI_API_KEY` and defaults to `gpt-4.1-mini`. Set
`OPENAI_MODEL` to select an OpenAI model that supports Structured Outputs.
`TRIAGE_MODEL_PROVIDER` selects `openai`, `ollama`, or `stub` for `diagnose` and `eval`.
An explicit `--provider` overrides the environment, and `demo` always uses the stub.

Run the evaluation with `OPENAI_API_KEY` configured in your environment.

```powershell
.\.venv\Scripts\python.exe -m triage eval --provider openai --output eval-real.json
```

Use `--provider stub --output eval-offline.json` for a separate offline pipeline
smoke report. Omitting `--output` writes the report to stdout. The report contains
one row per case with the diagnosis, retrieved IDs, selected log lines, submitted
category and citations, and scores. For incorrect positive cases, `miss_stage` shows
whether the labeled runbook was missing, the selected excerpts omitted the labeled
lines, the model had both sources of evidence, or citation validation rejected its
draft. Operational failures and misclassified unknown cases have their own stages.
The [evaluation manifest](triage/data/evaluation/held_out.json) holds three cases per
category, including three expected unknowns. `case-08` contains ambiguous signals.
The evaluator passes only the attempt identifiers to the workflow. Expected categories
and evidence annotations are used for scoring after each diagnosis.

| Metric | Numerator and denominator |
| --- | --- |
| Category accuracy | Correct final categories across all 12 cases |
| Runbook hit@3 | Cases with the labeled section among retrieved sections across the 9 positive cases |
| Unknown recall | Expected unknowns that abstained across the 3 expected unknowns |
| False abstentions | Positive cases that abstained across the 9 positive cases |
| Citation validity | Responses with valid citations across all responses that underwent a citation check |
| Labeled evidence match | Cases citing both the expected section and at least one labeled log line across the 9 positive cases |

Citation checks apply to positive drafts and any draft that supplies citations. The
report also gives the log and runbook evidence matches individually. A citation can
be valid against the supplied evidence while missing the labeled evidence. Rates
contain `count`, `total`, and `rate`, with `null` for a zero denominator. Tool and model
errors are counted separately and count as incorrect in category accuracy. All logs
and runbooks are synthetic. Scores apply to these fixtures only.

### Local evaluation with Ollama

Start Ollama and make sure `qwen2.5:3b` appears in `ollama list`. The provider uses
`ChatOllama` with JSON-schema structured output at `http://127.0.0.1:11434` by default.
Set `OLLAMA_MODEL` or `OLLAMA_BASE_URL` to override the model tag or endpoint.
[ChatOllama integration](https://docs.langchain.com/oss/python/integrations/chat/ollama) · [Ollama structured outputs](https://docs.ollama.com/capabilities/structured-outputs)

From the repository root, install the updated dependencies and run a fixture diagnosis
followed by the held-out benchmark.

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m triage diagnose --provider ollama --dag-id daily_sales --run-id 'manual__2025-01-11T06:00:00+00:00' --task-id load_daily_sales --try-number 1
.\.venv\Scripts\python.exe -m triage eval --provider ollama --output eval-ollama.json
```

## Airflow 3.1.6 demo

The demo uses a local Linux container through Docker Compose. On Windows, use Docker
Desktop with Linux containers enabled. From the repository root, run the following
after completing Python setup.

```powershell
docker compose version
$result = & .\.venv\Scripts\python.exe -m demo.run_airflow
if ($LASTEXITCODE -eq 0) {
  $result | Set-Content -Encoding utf8 demo-airflow-result.json
  $result
}
```

The script starts the Compose service, waits for the `triage_missing_input` DAG,
triggers a unique run, and reads the actual run and attempt IDs from Airflow. Its
`read_upstream_partition` task fails when it tries to open a missing Parquet file.
The script then calls `triage diagnose` over MCP stdio and compares the cited record
with a second request for the same attempt. Successful output contains `diagnosis`
and the actual API records in `verified_airflow_log_lines`. Airflow uses a local
admin only for triggering and passes a separate viewer token to the CLI. Compose
binds the API to loopback and generates demo passwords inside the container.

For a task attempt on an existing compatible instance, set `AIRFLOW_BASE_URL` and
`AIRFLOW_API_TOKEN` in your local environment. Use an account authorized for
`DAG.TASK_LOGS` reads and substitute the IDs returned by Airflow.

```powershell
.\.venv\Scripts\python.exe -m triage diagnose --provider stub --log-source airflow --dag-id $dagId --run-id $runId --task-id $taskId --try-number $tryNumber
```

The tool reads the public `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}` endpoint for an unmapped task and follows continuation tokens.
In Airflow mode `log_source` is that endpoint URL and cited numbers are 1-based
record positions across pages. Each record is serialized as one line for the model.
The adapter passed a loopback API test double. A sample from the actual DAG run
is pending a local Linux container runtime. [Airflow 3.1.6 public REST API](https://airflow.apache.org/docs/apache-airflow/3.1.6/stable-rest-api-ref.html)

To stop the local demo service, run the following command.

```powershell
docker compose -f demo/compose.yaml down
```

## OAuth-protected HTTP MCP

The MCP server acts as an OAuth resource server. Configure an external OAuth issuer
to issue RFC 9068 RS256 access tokens with `aud` containing the exact MCP resource URL
and the `logs:read` scope. Export the issuer's RSA signing **public** key as a PEM file
and update it when the issuer rotates its key. The server publishes protected
resource metadata, verifies the token signature, type, issuer, audience and expiry,
and uses SDK scope enforcement. Access without a valid token returns `401`. A valid
token without `logs:read` returns `403` and the required scope. The local listener
binds to loopback. [MCP authorization specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization) · [MCP Python SDK v1.30.0](https://github.com/modelcontextprotocol/python-sdk/tree/v1.30.0) · [JWT access token profile](https://www.rfc-editor.org/rfc/rfc9068.html)

In a PowerShell server terminal, configure your issuer and public key, then start
the fixture-backed HTTP endpoint.

```powershell
$env:TRIAGE_OAUTH_ISSUER = 'https://issuer.example/'
$env:TRIAGE_MCP_RESOURCE_URL = 'http://127.0.0.1:8001/mcp'
$env:TRIAGE_OAUTH_PUBLIC_KEY_FILE = 'C:\path\to\issuer-public.pem'
.\.venv\Scripts\python.exe -m triage.log_server --transport streamable-http --source fixtures
```

In the client terminal, set `TRIAGE_MCP_ACCESS_TOKEN` locally to an access token from
that issuer, then run the HTTP diagnosis.

```powershell
.\.venv\Scripts\python.exe -m triage diagnose --provider stub --mcp-url http://127.0.0.1:8001/mcp --dag-id daily_sales --run-id 'manual__2025-01-11T06:00:00+00:00' --task-id load_daily_sales --try-number 1
```

To read Airflow logs over HTTP, start the MCP server with `--source airflow` and set
`AIRFLOW_BASE_URL` and `AIRFLOW_API_TOKEN` in the server terminal. The server uses the
Airflow credential for upstream reads. The MCP access token is validated locally.
`python -m triage demo` uses fixture stdio and requires no tokens.

## API references

- Typed `FastMCP` output and the stdio client session. [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk/tree/v1.x)
- `TFIDFRetriever.from_documents` and runnable retrieval. [LangChain TF-IDF source](https://github.com/langchain-ai/langchain-community/blob/main/libs/community/langchain_community/retrievers/tfidf.py)
- `StateGraph` and asynchronous graph execution. [LangGraph graph API](https://docs.langchain.com/oss/python/langgraph/graph-api)
- Provider-native JSON schema and the default model. [LangChain structured output](https://reference.langchain.com/python/langchain-openai/chat_models/base/BaseChatOpenAI/with_structured_output) · [OpenAI GPT-4.1 mini](https://developers.openai.com/api/docs/models/gpt-4.1-mini)
