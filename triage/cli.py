import argparse
import asyncio
import json
import os
import sys
from functools import partial
from pathlib import Path

from pydantic import ValidationError

from .evaluation import evaluate
from .log_client import fetch_task_log
from .models import configured_model, stub_model
from .schemas import Attempt
from .workflow import build_graph

DEMO_ATTEMPTS = (
    Attempt(dag_id="daily_sales", run_id="manual__2025-01-11T06:00:00+00:00",
            task_id="load_daily_sales", try_number=1),
    Attempt(dag_id="daily_metrics", run_id="manual__2025-01-14T06:00:00+00:00",
            task_id="publish", try_number=1),
)
PROVIDERS = ("openai", "ollama", "stub")
ERROR_STATUSES = {"tool_error", "model_error"}


def write_json(payload: dict | list, path: Path | None = None) -> None:
    serialized = json.dumps(payload, indent=2) + "\n"
    if path is None:
        sys.stdout.write(serialized)
    else:
        path.write_text(serialized, encoding="utf-8")


async def run(args: argparse.Namespace) -> int:
    if args.command == "demo":
        graph = build_graph(stub_model)
        results = []
        for attempt in DEMO_ATTEMPTS:
            state = await graph.ainvoke({"attempt": attempt})
            results.append(state["diagnosis"].model_dump())
        write_json(results)
        return 2 if any(result["status"] in ERROR_STATUSES for result in results) else 0

    provider = args.provider or os.getenv("TRIAGE_MODEL_PROVIDER", "openai")
    if provider not in PROVIDERS:
        print(f"TRIAGE_MODEL_PROVIDER must be one of {', '.join(PROVIDERS)}.", file=sys.stderr)
        return 2
    try:
        name, model = configured_model(provider)
    except ValueError as exc:
        print(json.dumps({"status": "model_error", "explanation": str(exc)}), file=sys.stderr)
        return 2

    if args.command == "eval":
        report = await evaluate(name, model)
        write_json(report, args.output)
        return 2 if report["tool_errors"] or report["model_errors"] else 0

    attempt = Attempt(dag_id=args.dag_id, run_id=args.run_id, task_id=args.task_id,
                      try_number=args.try_number)
    reader = partial(fetch_task_log, source=args.log_source, mcp_url=args.mcp_url)
    state = await build_graph(model, reader).ainvoke({"attempt": attempt})
    diagnosis = state["diagnosis"]
    write_json(diagnosis.model_dump())
    return 2 if diagnosis.status in ERROR_STATUSES else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose a failed task from fixture or Airflow logs.")
    commands = parser.add_subparsers(dest="command", required=True)
    diagnose = commands.add_parser("diagnose", help="Diagnose one task attempt")
    diagnose.add_argument("--dag-id", required=True)
    diagnose.add_argument("--run-id", required=True)
    diagnose.add_argument("--task-id", required=True)
    diagnose.add_argument("--try-number", type=int, required=True)
    diagnose.add_argument("--provider", choices=PROVIDERS)
    source = diagnose.add_mutually_exclusive_group()
    source.add_argument("--log-source", choices=("fixtures", "airflow"), default="fixtures")
    source.add_argument("--mcp-url", help="Use the OAuth-protected HTTP MCP endpoint")
    commands.add_parser("demo", help="Run one known and one unknown case offline")
    evaluation = commands.add_parser("eval", help="Score the 12 held-out synthetic cases")
    evaluation.add_argument("--provider", choices=PROVIDERS)
    evaluation.add_argument("--output", type=Path, help="Write the JSON report to this file")
    try:
        return asyncio.run(run(parser.parse_args()))
    except (ValidationError, OSError) as exc:
        parser.exit(2, f"{exc}\n")
