import os
import sys
from contextlib import asynccontextmanager
from datetime import timedelta

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import TextContent
from pydantic import ValidationError

from .airflow_logs import AIRFLOW_ENV, http_url
from .schemas import Attempt, TaskLog


class LogToolError(Exception):
    """The MCP call did not return a usable task log."""


@asynccontextmanager
async def log_session(source: str = "fixtures"):
    env = {key: os.environ[key] for key in AIRFLOW_ENV if key in os.environ} if source == "airflow" else None
    params = StdioServerParameters(command=sys.executable,
                                   args=["-m", "triage.log_server", "--source", source], env=env)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=15)) as session:
            await session.initialize()
            yield session


@asynccontextmanager
async def http_log_session(url: str):
    url = http_url(url, "MCP URL")
    token = os.getenv("TRIAGE_MCP_ACCESS_TOKEN")
    if not token:
        raise LogToolError("TRIAGE_MCP_ACCESS_TOKEN is required for HTTP MCP.")
    async with httpx.AsyncClient(headers={"Authorization": f"Bearer {token}"}, timeout=15,
                                 trust_env=False, follow_redirects=False) as client:
        async with streamable_http_client(url, http_client=client) as (read, write, _):
            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=15)) as session:
                await session.initialize()
                yield session


async def fetch_task_log(attempt: Attempt, *, source: str = "fixtures", mcp_url: str | None = None) -> TaskLog:
    try:
        async with (http_log_session(mcp_url) if mcp_url else log_session(source)) as session:
            result = await session.call_tool("get_task_log", arguments=attempt.model_dump())
    except LogToolError:
        raise
    except Exception as exc:
        cause = exc
        while isinstance(cause, BaseExceptionGroup) and len(cause.exceptions) == 1:
            cause = cause.exceptions[0]
        raise LogToolError(f"MCP request failed ({type(cause).__name__}): {cause}") from exc

    if result.isError:
        detail = " ".join(item.text for item in result.content if isinstance(item, TextContent))
        raise LogToolError(detail or "MCP get_task_log returned an error.")
    try:
        return TaskLog.model_validate(result.structuredContent)
    except ValidationError as exc:
        raise LogToolError("MCP get_task_log returned an invalid structured payload.") from exc
