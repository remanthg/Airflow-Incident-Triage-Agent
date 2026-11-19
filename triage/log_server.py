import argparse
import asyncio
import json
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from .airflow_logs import airflow_log
from .schemas import Attempt, NumberedLine, TaskLog

DATA_DIR = Path(__file__).resolve().parent / "data"
FIXTURE_DIR = DATA_DIR / "fixtures"


def fixture_index() -> dict[Attempt, Path]:
    rows = json.loads((FIXTURE_DIR / "index.json").read_text(encoding="utf-8"))
    indexed: dict[Attempt, Path] = {}
    log_dir = FIXTURE_DIR / "logs"
    for row in rows:
        attempt = Attempt.model_validate({key: row[key] for key in Attempt.model_fields})
        path = (FIXTURE_DIR / row["log_file"]).resolve()
        if not path.is_relative_to(log_dir) or attempt in indexed:
            raise ValueError("Fixture index contains an invalid path or a duplicate attempt.")
        indexed[attempt] = path
    return indexed


def fixture_log(attempt: Attempt) -> TaskLog:
    path = fixture_index().get(attempt)
    if path is None:
        raise ToolError(f"Task attempt not found in fixture index: {attempt.model_dump_json()}")
    lines = path.read_text(encoding="utf-8").splitlines()
    return TaskLog(
        source=path.relative_to(DATA_DIR.parent.parent).as_posix(),
        lines=[NumberedLine(number=number, text=line) for number, line in enumerate(lines, 1)],
    )


def create_server(source: str, **settings) -> FastMCP:
    reader = {"fixtures": fixture_log, "airflow": airflow_log}[source]
    server = FastMCP("local-airflow-task-logs", log_level="ERROR", **settings)

    @server.tool(annotations=ToolAnnotations(readOnlyHint=True, openWorldHint=False))
    async def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int) -> TaskLog:
        """Read a task attempt log with stable, 1-based citation numbers."""
        attempt = Attempt(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number)
        return await asyncio.to_thread(reader, attempt)

    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve read-only Airflow attempt logs over MCP.")
    parser.add_argument("--source", choices=("fixtures", "airflow"), default="fixtures")
    parser.add_argument("--transport", choices=("stdio", "streamable-http"), default="stdio")
    parser.add_argument("--port", type=int, default=8001, help="Loopback HTTP listen port")
    args = parser.parse_args()
    if args.transport == "stdio":
        create_server(args.source).run(transport="stdio")
        return

    import uvicorn

    from .http_auth import configured_http_app

    try:
        app = configured_http_app(args.source)
    except (ValueError, OSError) as exc:
        parser.exit(2, f"{exc}\n")
    uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
