import json
from collections import Counter
from pathlib import Path

from pydantic import BaseModel, PositiveInt, model_validator

from .models import DiagnosisModel
from .schemas import Attempt, Category, Diagnosis
from .workflow import build_graph

MANIFEST = Path(__file__).resolve().parent / "data" / "evaluation" / "held_out.json"


class EvalCase(BaseModel):
    case_id: str
    attempt: Attempt
    expected_category: Category
    expected_runbook_section: str | None
    supporting_log_lines: list[PositiveInt]

    @model_validator(mode="after")
    def check_evidence(self):
        if self.expected_category == "unknown":
            if self.expected_runbook_section or self.supporting_log_lines:
                raise ValueError("Unknown cases must have empty evidence annotations.")
        elif not self.expected_runbook_section or not self.supporting_log_lines:
            raise ValueError("Positive cases require both evidence annotations.")
        return self


def rate(count: int, total: int) -> dict:
    return {"count": count, "total": total, "rate": count / total if total else None}


def miss_stage(case: EvalCase, result: Diagnosis, sections: list[str], lines: list[int]) -> str | None:
    if result.status in {"tool_error", "model_error"}:
        return result.status
    if result.category == case.expected_category:
        return None
    if case.expected_category != "unknown":
        if case.expected_runbook_section not in sections:
            return "runbook_retrieval_miss"
        if not set(case.supporting_log_lines).intersection(lines):
            return "log_excerpt_miss"
    if result.citation_status == "invalid":
        return "citation_validation_failed"
    if case.expected_category == "unknown":
        return "unknown_misclassified"
    return "model_decision_with_labeled_evidence"


async def evaluate(name: str, model: DiagnosisModel) -> dict:
    cases = [EvalCase.model_validate(row) for row in json.loads(MANIFEST.read_text(encoding="utf-8"))]
    graph = build_graph(model)
    rows = []
    for case in cases:
        state = await graph.ainvoke({"attempt": case.attempt})
        result = state["diagnosis"]
        evidence = state.get("evidence")
        draft = state.get("draft")
        retrieved = [section.section_id for section in evidence.runbook_sections] if evidence else []
        excerpts = [line.number for line in evidence.log_lines] if evidence else []
        positive = case.expected_category != "unknown"
        log_match = bool(set(case.supporting_log_lines).intersection(result.cited_log_lines))
        section_match = case.expected_runbook_section in result.cited_runbook_sections
        rows.append({
            **case.model_dump(exclude={"attempt"}),
            "diagnosis": result.model_dump(),
            "retrieved_runbook_sections": retrieved,
            "selected_log_lines": excerpts,
            "miss_stage": miss_stage(case, result, retrieved, excerpts),
            "category_correct": (
                result.status in {"diagnosed", "abstained"} and result.category == case.expected_category
            ),
            "runbook_hit_at_3": case.expected_runbook_section in retrieved if positive else None,
            "labeled_log_match": log_match if positive else None,
            "labeled_runbook_match": section_match if positive else None,
            "labeled_evidence_match": log_match and section_match if positive else None,
            "submitted_category": draft.category if draft else None,
            "submitted_citations": draft.model_dump(include={"cited_log_lines", "cited_runbook_sections"})
            if draft else None,
        })

    positives = [row for row in rows if row["expected_category"] != "unknown"]
    unknowns = [row for row in rows if row["expected_category"] == "unknown"]
    statuses = Counter(row["diagnosis"]["status"] for row in rows)
    citations = Counter(row["diagnosis"]["citation_status"] for row in rows)
    return {
        "model": name,
        "mode": "offline_pipeline_smoke" if name == "stub" else "live_model_evaluation",
        "data": "synthetic_held_out",
        "metrics": {
            "category_accuracy": rate(sum(row["category_correct"] for row in rows), len(rows)),
            "runbook_hit_at_3": rate(sum(row["runbook_hit_at_3"] for row in positives), len(positives)),
            "unknown_recall": rate(sum(row["category_correct"] for row in unknowns), len(unknowns)),
            "false_abstentions": rate(
                sum(row["diagnosis"]["status"] == "abstained" for row in positives), len(positives)
            ),
            "citation_validity": rate(citations["valid"], citations["valid"] + citations["invalid"]),
            "labeled_log_match": rate(sum(row["labeled_log_match"] for row in positives), len(positives)),
            "labeled_runbook_match": rate(
                sum(row["labeled_runbook_match"] for row in positives), len(positives)
            ),
            "labeled_evidence_match": rate(
                sum(row["labeled_evidence_match"] for row in positives), len(positives)
            ),
        },
        "tool_errors": statuses["tool_error"],
        "model_errors": statuses["model_error"],
        "citation_checks_not_applicable": citations["not_applicable"],
        "misses_by_stage": dict(Counter(row["miss_stage"] for row in rows if row["miss_stage"])),
        "cases": rows,
    }
