from collections.abc import Awaitable, Callable
from typing import NotRequired, TypedDict

from langgraph.graph import END, START, StateGraph

from .log_client import LogToolError, fetch_task_log
from .models import DiagnosisModel, Evidence, RunbookSection
from .retrieval import error_excerpts, retrieve_sections, runbook_retriever
from .schemas import Attempt, CitationStatus, Diagnosis, DiagnosisDraft, Status, TaskLog

NEXT_CHECK = {
    "missing_upstream_data": "Inspect the upstream producer attempt for the expected data interval and output path.",
    "task_timeout": "Compare the task attempt duration with its configured execution timeout.",
    "dependency_import_error": "Inspect the failing worker image's package inventory for the named dependency.",
    "unknown": "Inspect the full task attempt log for the first recorded cause before cleanup.",
}


class TriageState(TypedDict):
    attempt: Attempt
    task_log: NotRequired[TaskLog]
    evidence: NotRequired[Evidence]
    draft: NotRequired[DiagnosisDraft]
    diagnosis: NotRequired[Diagnosis]


def unknown(
    state: TriageState, reason: str, *, status: Status = "abstained",
    citation_status: CitationStatus = "not_applicable",
) -> Diagnosis:
    task_log = state.get("task_log")
    return Diagnosis(
        **state["attempt"].model_dump(), category="unknown", explanation=reason,
        cited_log_lines=[], cited_runbook_sections=[], status=status,
        citation_status=citation_status, log_source=task_log.source if task_log else None,
        next_check=NEXT_CHECK["unknown"] if status == "abstained" else None,
    )


def build_graph(model: DiagnosisModel, log_reader: Callable[[Attempt], Awaitable[TaskLog]] = fetch_task_log):
    retriever = runbook_retriever()

    async def fetch_logs(state: TriageState) -> dict:
        try:
            return {"task_log": await log_reader(state["attempt"])}
        except LogToolError as exc:
            return {"diagnosis": unknown(state, str(exc), status="tool_error")}

    def retrieve_runbooks(state: TriageState) -> dict:
        if "diagnosis" in state:
            return {}
        excerpts = error_excerpts(state["task_log"].lines)
        sections = [
            RunbookSection(doc.metadata["section_id"], doc.page_content)
            for doc in retrieve_sections(retriever, state["attempt"], excerpts)
        ] if excerpts else []
        return {"evidence": Evidence(state["attempt"], excerpts, sections)}

    async def generate_diagnosis(state: TriageState) -> dict:
        if "diagnosis" in state:
            return {}
        evidence = state["evidence"]
        if not evidence.log_lines or not evidence.runbook_sections:
            return {"diagnosis": unknown(state, "Insufficient evidence: a log error excerpt and a matching runbook section are required.")}
        try:
            draft = DiagnosisDraft.model_validate(await model(evidence))
            if not draft.explanation.strip():
                raise ValueError("Empty model explanation")
            return {"draft": draft}
        except Exception as exc:
            reason = f"Model request or structured response failed ({type(exc).__name__})."
            return {"diagnosis": unknown(state, reason, status="model_error")}

    def validate_citations(state: TriageState) -> dict:
        if "diagnosis" in state:
            return {}
        draft, evidence = state["draft"], state["evidence"]
        lines, sections = set(draft.cited_log_lines), set(draft.cited_runbook_sections)
        bad_lines = lines - {line.number for line in evidence.log_lines}
        bad_sections = sections - {section.section_id for section in evidence.runbook_sections}
        positive = draft.category != "unknown"
        if bad_lines or bad_sections:
            reason = "Citation validation failed: a cited log line or runbook section was outside the model evidence."
        elif positive and (not lines or not sections):
            reason = "Citation validation failed: a positive diagnosis requires both log and runbook citations."
        else:
            return {"diagnosis": Diagnosis(
                **state["attempt"].model_dump(), **draft.model_dump(),
                next_check=NEXT_CHECK[draft.category], log_source=state["task_log"].source,
                status="diagnosed" if positive else "abstained",
                citation_status="valid" if lines or sections else "not_applicable",
            )}
        return {"diagnosis": unknown(state, reason, citation_status="invalid")}

    graph = StateGraph(TriageState)
    graph.add_node("fetch_logs", fetch_logs)
    graph.add_node("retrieve_runbooks", retrieve_runbooks)
    graph.add_node("generate_diagnosis", generate_diagnosis)
    graph.add_node("validate_citations", validate_citations)
    graph.add_edge(START, "fetch_logs")
    graph.add_edge("fetch_logs", "retrieve_runbooks")
    graph.add_edge("retrieve_runbooks", "generate_diagnosis")
    graph.add_edge("generate_diagnosis", "validate_citations")
    graph.add_edge("validate_citations", END)
    return graph.compile()
