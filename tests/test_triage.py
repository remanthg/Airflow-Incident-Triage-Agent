import asyncio
import json

import pytest

from triage.cli import DEMO_ATTEMPTS
from triage.evaluation import MANIFEST, evaluate
from triage.log_client import log_session
from triage.models import stub_model
from triage.schemas import DiagnosisDraft, TaskLog
from triage.workflow import build_graph


def test_mcp_round_trip():
    async def check():
        async with log_session() as session:
            tools = (await session.list_tools()).tools
            assert [tool.name for tool in tools] == ["get_task_log"]
            assert tools[0].annotations.readOnlyHint is True
            response = await session.call_tool("get_task_log", DEMO_ATTEMPTS[0].model_dump())
        assert not response.isError
        log = TaskLog.model_validate(response.structuredContent)
        assert log.source == "triage/data/fixtures/logs/dev-01.log"
        assert [line.number for line in log.lines] == [1, 2, 3, 4, 5]
        assert "FileNotFoundError" in log.lines[3].text

    asyncio.run(check())


def test_missing_attempt_is_a_tool_error():
    attempt = DEMO_ATTEMPTS[0].model_copy(update={"try_number": 9})
    result = asyncio.run(build_graph(stub_model).ainvoke({"attempt": attempt}))["diagnosis"]
    assert result.category == "unknown"
    assert result.status == "tool_error"
    assert "not found in fixture index" in result.explanation
    assert result.next_check is None


def test_insufficient_evidence_abstains_before_model_call():
    called = False

    async def unexpected_model(_):
        nonlocal called
        called = True
        raise AssertionError("The model received insufficient evidence.")

    result = asyncio.run(build_graph(unexpected_model).ainvoke({"attempt": DEMO_ATTEMPTS[1]}))["diagnosis"]
    assert result.category == "unknown"
    assert result.status == "abstained"
    assert "Insufficient evidence" in result.explanation
    assert result.cited_log_lines == result.cited_runbook_sections == []
    assert not called


@pytest.mark.parametrize("replace", [{"cited_log_lines": [1]}, {"cited_runbook_sections": ["RB-999"]},
                                    {"cited_log_lines": []}])
def test_validation_rejects_citations_outside_model_evidence_or_missing(replace):
    async def invalid_model(evidence):
        draft = await stub_model(evidence)
        assert draft.category == "missing_upstream_data"
        return draft.model_copy(update=replace)

    state = asyncio.run(build_graph(invalid_model).ainvoke({"attempt": DEMO_ATTEMPTS[0]}))
    assert state["task_log"].lines[0].number == 1
    assert 1 not in {line.number for line in state["evidence"].log_lines}
    result = state["diagnosis"]
    assert result.category == "unknown"
    assert result.status == "abstained"
    assert result.citation_status == "invalid"
    assert result.cited_log_lines == result.cited_runbook_sections == []


def test_positive_diagnosis_uses_exact_log_and_runbook_citations():
    result = asyncio.run(build_graph(stub_model).ainvoke({"attempt": DEMO_ATTEMPTS[0]}))["diagnosis"]
    assert result.category == "missing_upstream_data"
    assert result.status == "diagnosed"
    assert result.cited_log_lines == [4]
    assert result.cited_runbook_sections == ["RB-101"]
    assert result.citation_status == "valid"


def test_model_failure_is_distinct_from_abstention():
    async def broken_model(_) -> DiagnosisDraft:
        raise TimeoutError("test timeout")

    result = asyncio.run(build_graph(broken_model).ainvoke({"attempt": DEMO_ATTEMPTS[0]}))["diagnosis"]
    assert result.category == "unknown"
    assert result.status == "model_error"
    assert "TimeoutError" in result.explanation


def test_offline_evaluation_keeps_citation_validity_and_gold_match_separate(monkeypatch, tmp_path):
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    case = manifest[2]
    case["supporting_log_lines"] = [3]
    path = tmp_path / "cases.json"
    path.write_text(json.dumps([case]), encoding="utf-8")
    monkeypatch.setattr("triage.evaluation.MANIFEST", path)
    report = asyncio.run(evaluate("stub", stub_model))
    assert report["mode"] == "offline_pipeline_smoke"
    assert report["metrics"]["category_accuracy"] == {"count": 1, "total": 1, "rate": 1.0}
    assert report["metrics"]["citation_validity"] == {"count": 1, "total": 1, "rate": 1.0}
    assert report["metrics"]["labeled_evidence_match"] == {"count": 0, "total": 1, "rate": 0.0}


def test_eval_attributes_missed_diagnoses_to_the_available_evidence(monkeypatch, tmp_path):
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    case = manifest[0]
    missed_section = {**case, "expected_runbook_section": "RB-999"}
    missed_excerpt = {**case, "supporting_log_lines": [1]}
    path = tmp_path / "cases.json"
    path.write_text(json.dumps([case, missed_section, missed_excerpt]), encoding="utf-8")
    monkeypatch.setattr("triage.evaluation.MANIFEST", path)

    async def abstain(_):
        return DiagnosisDraft(category="unknown", explanation="Abstaining for test.",
                              cited_log_lines=[], cited_runbook_sections=[])

    report = asyncio.run(evaluate("stub", abstain))
    assert [row["miss_stage"] for row in report["cases"]] == [
        "model_decision_with_labeled_evidence", "runbook_retrieval_miss", "log_excerpt_miss",
    ]
