"""
QA셋 생성 + LLM judge 평가.

문서를 3페이지(또는 ~6000자) 단위 윈도우로 쪼개 병렬로 LLM에게 보여주고
"이 윈도우를 봐서 낼 수 있는 Q / 반드시 나와야 할 핵심 A / 근거 페이지" 를
뽑게 한다. RAG 벤치마크 상황을 프롬프트에 명시해 답이 문서 내부에 실존하는
Q만 생성하도록 유도한다.

평가는 동일한 LLM을 judge로 사용해 0~5점 정수를 매긴다.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from langchain.schema import Document


# ---------------------------------------------------------------------------
# QA 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class QAItem:
    question: str
    expected_answer: str
    # 근거가 된 페이지(1-based). 비-PDF는 윈도우 인덱스(0-based)가 들어간다.
    source_pages: List[int]
    window_index: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# 윈도우 빌더
# ---------------------------------------------------------------------------

# 한 윈도우당 PDF는 3페이지, 다른 포맷은 이 글자수로 자른다.
WINDOW_PAGES = 3
WINDOW_CHARS = 6000


def build_windows(documents: List[Document]) -> List[Dict[str, Any]]:
    """
    반환 형태: [{"index": 0, "pages": [1,2,3], "text": "..."}]
    - PDF처럼 Document 하나 == 한 페이지인 경우 WINDOW_PAGES씩 묶는다.
    - 단일 Document(통문서)인 경우 WINDOW_CHARS 기준으로 자른다.
    """
    is_paged = len(documents) > 1 and all(
        ("page" in (d.metadata or {})) for d in documents
    )

    windows: List[Dict[str, Any]] = []
    if is_paged:
        for i in range(0, len(documents), WINDOW_PAGES):
            group = documents[i : i + WINDOW_PAGES]
            pages = [int(g.metadata.get("page", i)) + 1 for g in group]
            text = "\n\n".join((g.page_content or "") for g in group).strip()
            if text:
                windows.append({"index": len(windows), "pages": pages, "text": text})
        return windows

    full_text = "\n\n".join((d.page_content or "") for d in documents).strip()
    if not full_text:
        return []
    for i in range(0, len(full_text), WINDOW_CHARS):
        chunk = full_text[i : i + WINDOW_CHARS]
        if chunk.strip():
            windows.append(
                {"index": len(windows), "pages": [len(windows)], "text": chunk}
            )
    return windows


# ---------------------------------------------------------------------------
# QA 생성
# ---------------------------------------------------------------------------

QA_PROMPT = """당신은 RAG(Retrieval-Augmented Generation) 시스템의 **벤치마크용 QA셋**을
만드는 평가자입니다. 아래 문서 일부(윈도우)를 읽고, 이 문서 전체를 대상으로
RAG 시스템에 던졌을 때 **이 윈도우 안의 정보로 답해야 하는** 질문을 만듭니다.

조건:
- 질문은 자연스러운 한국어 단일 문장.
- 질문-답변은 모두 이 윈도우 텍스트에 **실존하는 사실**에 근거해야 합니다.
  상식·일반 지식으로 답하는 질문은 금지.
- 답변은 1~3문장, 핵심 사실만 간결하게.
- 질문은 서로 다른 내용을 다루도록(중복 금지).
- 애매하거나 근거가 약하면 질문 수를 줄이세요. 질 > 양.

최대 {max_q}개의 QA를 JSON 배열로만 출력하세요. 다른 말 금지.
형식:
[
  {{"question": "...", "answer": "..."}},
  ...
]

[윈도우 페이지: {pages}]
[윈도우 텍스트]
{text}
"""


def _extract_json_array(raw: str) -> List[Dict[str, str]]:
    """LLM 응답에서 JSON 배열만 뽑아낸다. 코드펜스·잡설이 섞여도 견고하게."""
    if not raw:
        return []
    # 코드펜스 제거
    fenced = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    else:
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if m:
            raw = m.group(0)
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        q = (item.get("question") or "").strip()
        a = (item.get("answer") or item.get("expected_answer") or "").strip()
        if q and a:
            out.append({"question": q, "answer": a})
    return out


async def _generate_for_window(
    llm, window: Dict[str, Any], max_q: int, sem: asyncio.Semaphore
) -> List[QAItem]:
    prompt = QA_PROMPT.format(
        pages=window["pages"], text=window["text"][:WINDOW_CHARS], max_q=max_q
    )
    async with sem:
        try:
            resp = await llm.ainvoke(prompt)
            text = getattr(resp, "content", resp)
            if not isinstance(text, str):
                text = str(text)
        except Exception as e:
            print(f"  [qa-gen] window {window['index']} failed: {e}")
            return []

    raw_items = _extract_json_array(text)
    return [
        QAItem(
            question=it["question"],
            expected_answer=it["answer"],
            source_pages=list(window["pages"]),
            window_index=window["index"],
        )
        for it in raw_items[:max_q]
    ]


def _qa_per_window(total_windows: int) -> int:
    """윈도우 수에 따라 윈도우당 QA 개수를 적응적으로 정한다.

    - 2~3페이지짜리 짧은 문서: 윈도우 1~2개 → 3문항
    - 보통(~30페이지): 윈도우 10개 → 2문항씩 ≈ 20문항
    - 80페이지 이상: 윈도우당 1문항으로 줄여 과도한 대기시간 방지
    """
    if total_windows <= 2:
        return 3
    if total_windows <= 10:
        return 2
    return 1


async def generate_qa_set(llm, documents: List[Document]) -> List[QAItem]:
    windows = build_windows(documents)
    if not windows:
        return []

    max_q = _qa_per_window(len(windows))
    print(f"[qa-gen] windows={len(windows)}, qa_per_window={max_q}")

    sem = asyncio.Semaphore(5)
    tasks = [_generate_for_window(llm, w, max_q, sem) for w in windows]
    grouped = await asyncio.gather(*tasks)

    qas: List[QAItem] = []
    for g in grouped:
        qas.extend(g)
    print(f"[qa-gen] generated {len(qas)} QA items")
    return qas


# ---------------------------------------------------------------------------
# 평가: LLM judge
# ---------------------------------------------------------------------------

JUDGE_PROMPT = """당신은 RAG 시스템 답변 품질을 채점하는 평가자입니다.
아래 '질문', '정답(Ground Truth)', 'RAG 시스템 답변'을 보고 0~5점 정수로 채점하세요.

채점 기준:
- 5: 정답의 핵심 사실을 모두 담고 추가 오류 없음
- 4: 핵심 사실을 담았으나 사소한 누락/모호함
- 3: 핵심 일부만 담거나 부정확한 보조 정보 포함
- 2: 관련은 있지만 핵심을 빗겨감
- 1: 거의 관련 없음
- 0: 완전히 틀렸거나 "모른다"

JSON 한 줄로만 출력: {{"score": <0-5>, "reason": "<한 문장>"}}

[질문]
{question}

[정답]
{expected}

[RAG 답변]
{actual}
"""


async def judge_answer(llm, question: str, expected: str, actual: str) -> Dict[str, Any]:
    prompt = JUDGE_PROMPT.format(question=question, expected=expected, actual=actual)
    try:
        resp = await llm.ainvoke(prompt)
        text = getattr(resp, "content", resp)
        if not isinstance(text, str):
            text = str(text)
    except Exception as e:
        return {"score": 0, "reason": f"judge error: {e}"}

    m = re.search(r"\{.*?\}", text, re.DOTALL)
    if not m:
        return {"score": 0, "reason": "no json in judge output"}
    try:
        data = json.loads(m.group(0))
        score = int(data.get("score", 0))
        score = max(0, min(5, score))
        return {"score": score, "reason": str(data.get("reason", ""))[:200]}
    except Exception as e:
        return {"score": 0, "reason": f"parse error: {e}"}


def retrieval_recall(retrieved_docs: List[Document], source_pages: List[int]) -> float:
    """검색된 청크의 page_number 중 정답 페이지가 얼마나 포함됐는지 (0~1)."""
    if not source_pages:
        return 0.0
    retrieved_pages = set()
    for d in retrieved_docs:
        meta = d.metadata or {}
        p = meta.get("page_number")
        if p is None:
            p = meta.get("page")
            if isinstance(p, int):
                p = p + 1
        try:
            retrieved_pages.add(int(p))
        except (TypeError, ValueError):
            continue
    hits = sum(1 for sp in source_pages if sp in retrieved_pages)
    return hits / len(source_pages)
