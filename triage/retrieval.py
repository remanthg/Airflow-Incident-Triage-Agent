import re
from pathlib import Path

from langchain_community.retrievers import TFIDFRetriever
from langchain_core.documents import Document

from .schemas import Attempt, NumberedLine

RUNBOOK_DIR = Path(__file__).resolve().parent / "data" / "runbooks"
SECTION = re.compile(r"(?m)^## (RB-\d+) \| (.+)$")
FAILURE = re.compile(
    r"\b(?:ERROR|CRITICAL|FATAL)\b|Traceback|\b\w*(?:Error|Exception)\s*:"
)


def runbook_retriever() -> TFIDFRetriever:
    documents: list[Document] = []
    ids: set[str] = set()
    for path in sorted(RUNBOOK_DIR.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        matches = list(SECTION.finditer(text))
        if len(matches) != len(re.findall(r"(?m)^## ", text)) or not matches:
            raise ValueError(f"Each section in {path.name} must have a stable ID.")
        for index, match in enumerate(matches):
            section_id, title = match.groups()
            if section_id in ids:
                raise ValueError(f"Duplicate runbook section ID: {section_id}")
            ids.add(section_id)
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            body = text[match.end():end].strip()
            documents.append(Document(
                page_content=f"{title}\n{body}", metadata={"section_id": section_id}
            ))
    return TFIDFRetriever.from_documents(
        documents, k=3, tfidf_params={"ngram_range": (1, 2), "stop_words": "english"}
    )


def error_excerpts(lines: list[NumberedLine]) -> list[NumberedLine]:
    anchors = [index for index, line in enumerate(lines) if FAILURE.search(line.text)]
    included = {
        index
        for anchor in anchors[-12:]
        for index in range(max(0, anchor - 1), min(len(lines), anchor + 2))
    }
    return [lines[index] for index in sorted(included)]


def retrieve_sections(
    retriever: TFIDFRetriever, attempt: Attempt, excerpts: list[NumberedLine]
) -> list[Document]:
    context = re.sub(r"[_.-]", " ", f"{attempt.dag_id} {attempt.task_id}")
    query = "\n".join([context, *(line.text for line in excerpts)])
    tokens = retriever.vectorizer.build_analyzer()
    query_terms = set(tokens(query))
    # TF-IDF returns up to k documents even when some have zero similarity.
    return [
        doc for doc in retriever.invoke(query)
        if query_terms.intersection(tokens(doc.page_content))
    ]
