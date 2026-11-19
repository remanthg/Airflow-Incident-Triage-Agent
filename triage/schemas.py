from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, StringConstraints

Category = Literal["missing_upstream_data", "task_timeout", "dependency_import_error", "unknown"]
Status = Literal["diagnosed", "abstained", "tool_error", "model_error"]
CitationStatus = Literal["valid", "invalid", "not_applicable"]
Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class Attempt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dag_id: Identifier
    run_id: Identifier
    task_id: Identifier
    try_number: PositiveInt


class NumberedLine(BaseModel):
    number: PositiveInt
    text: str


class TaskLog(BaseModel):
    source: str
    lines: list[NumberedLine]


class DiagnosisDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: Category
    explanation: str
    cited_log_lines: list[int]
    cited_runbook_sections: list[str]


class Diagnosis(Attempt, DiagnosisDraft):
    next_check: str | None
    status: Status
    citation_status: CitationStatus
    log_source: str | None
