import json
import re
from collections import Counter
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "triage" / "data"
ATTEMPT_FIELDS = ("dag_id", "run_id", "task_id", "try_number")


def attempt_key(row):
    return tuple(row[field] for field in ATTEMPT_FIELDS)


def test_holdout_labels_and_evidence_reference_disjoint_indexed_fixtures():
    index = json.loads((DATA_DIR / "fixtures" / "index.json").read_text(encoding="utf-8"))
    by_attempt = {attempt_key(row): row["log_file"] for row in index}
    assert len(by_attempt) == len(index)
    cases = json.loads((DATA_DIR / "evaluation" / "held_out.json").read_text(encoding="utf-8"))
    assert len({case["case_id"] for case in cases}) == len(cases) == 12
    assert len({attempt_key(case["attempt"]) for case in cases}) == len(cases)
    assert Counter(case["expected_category"] for case in cases) == {
        "missing_upstream_data": 3, "task_timeout": 3, "dependency_import_error": 3, "unknown": 3,
    }
    sections = [
        section
        for path in (DATA_DIR / "runbooks").glob("*.md")
        for section in re.findall(r"(?m)^## (RB-\d+) \|", path.read_text(encoding="utf-8"))
    ]
    assert len(sections) == len(set(sections))
    development_logs = set(by_attempt.values()) - {
        by_attempt[attempt_key(case["attempt"])] for case in cases
    }
    assert development_logs == {f"logs/dev-{index:02}.log" for index in range(1, 5)}
    categories = {case["expected_category"] for case in cases}
    for case in cases:
        path = DATA_DIR / "fixtures" / by_attempt[attempt_key(case["attempt"])]
        text = path.read_text(encoding="utf-8")
        assert not any(category in text for category in categories)
        assert not re.search(r"RB-\d+", text)
        numbers = case["supporting_log_lines"]
        if case["expected_category"] == "unknown":
            assert case["expected_runbook_section"] is None and not numbers
        else:
            assert case["expected_runbook_section"] in sections and numbers
            assert all(0 < number <= len(text.splitlines()) for number in numbers)
