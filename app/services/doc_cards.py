"""문서 카드 — 문서 전체를 슬라이딩 윈도우로 읽어 만드는 리트리벌 표면.

기존 abstract 는 앞 3쪽 + 뒤 1쪽만 보고 한 줄을 만들었다. 300쪽 문서에서 그 한 줄은
"무엇에 대한 문서인가"만 답하고, 에이전트가 정작 알아야 할 "이 문서를 열어야 하는가"는
답하지 못한다. 여기서는 텍스트를 *길이로* 잘라 순서대로 읽으며 카드를 갱신한다.
목차·헤딩·페이지 구조를 가정하지 않으므로 스캔 PDF·엑셀·메일 뭉치도 같은 경로를 탄다.

카드는 ``knowledge_doc_cards`` 에 저장한다. 그 테이블이 아직 없으면 요약만
``knowledge_files.doc_card`` 에 남겨 기존 화면이 계속 동작한다.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# 폴더 업로드는 수백 건을 한 번에 넣는다. 파일마다 카드 빌드를 던지면 LLM 이 그 수만큼
# 동시 호출을 받아 전부 타임아웃한다(300건 업로드에서 실제로 그렇게 됐다). 카드는 급하지
# 않으므로 몇 개씩만 돌린다. 다만 서버가 배칭을 잘해서 동시 3보다 6이 건당 26초→11초로
# 빨랐다 — 무제한과 직렬 사이의 값이다.
CARD_CONCURRENCY = max(1, int(os.getenv("KB_CARD_CONCURRENCY", "6")))
_card_gate: "asyncio.Semaphore | None" = None


def card_gate() -> "asyncio.Semaphore":
    global _card_gate
    if _card_gate is None:
        _card_gate = asyncio.Semaphore(CARD_CONCURRENCY)
    return _card_gate


# 사내 GPU 서버는 6,000자와 12,000자 프롬프트의 지연이 거의 같다(28.0초 vs 29.8초).
# 창을 키우면 같은 문서를 절반의 호출로 읽는다.
WINDOW_CHARS = int(os.getenv("KB_CARD_WINDOW_CHARS", "12000"))
WINDOW_SLACK = 400
MAX_WINDOWS = int(os.getenv("KB_CARD_MAX_WINDOWS", "16"))
MAX_LABELS = 12
MAX_QUESTIONS = 10
LABEL_MAX_CHARS = 60
QUESTION_MAX_CHARS = 160
CARD_VERSION = 2


@dataclass(frozen=True)
class Window:
    index: int
    total: int
    start: int
    end: int
    text: str


@dataclass
class Coverage:
    chars_total: int = 0
    chars_read: int = 0
    windows_total: int = 0
    windows_read: int = 0
    windows_contributed: int = 0
    windows_failed: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "chars_total": self.chars_total,
            "chars_read": self.chars_read,
            "windows_total": self.windows_total,
            "windows_read": self.windows_read,
            "windows_contributed": self.windows_contributed,
            "windows_failed": self.windows_failed,
            "whole_document": self.windows_read >= self.windows_total and self.windows_total > 0,
        }


@dataclass
class DocumentCard:
    title: str = ""
    summary: str = ""
    doc_type: str = ""
    language: str = "unknown"
    topics: List[str] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    answers_questions: List[str] = field(default_factory=list)
    coverage: Coverage = field(default_factory=Coverage)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "card_version": CARD_VERSION,
            "title": self.title,
            "summary": self.summary,
            "doc_type": self.doc_type,
            "language": self.language,
            "topics": self.topics,
            "entities": self.entities,
            "keywords": self.keywords,
            "answers_questions": self.answers_questions,
            "coverage": self.coverage.as_dict(),
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }


def make_windows(text: str, size: int = WINDOW_CHARS, slack: int = WINDOW_SLACK) -> List[Window]:
    """길이로 자르되 줄바꿈·공백에서 끊는다. 문서 구조를 묻지 않는다."""
    text = text or ""
    if not text.strip():
        return []
    bounds: List[Tuple[int, int]] = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            snap = text.rfind("\n", max(start, end - slack), end)
            if snap < 0:
                snap = text.rfind(" ", max(start, end - slack), end)
            if snap > start:
                end = snap + 1
        bounds.append((start, end))
        start = end
    return [
        Window(index=i, total=len(bounds), start=s, end=e, text=text[s:e])
        for i, (s, e) in enumerate(bounds)
    ]


def evenly_spaced(windows: Sequence[Window], limit: int) -> List[Window]:
    """예산을 넘으면 앞부분만 읽지 말고 문서 전체에 고르게 흩어 읽는다."""
    if limit <= 0 or len(windows) <= limit:
        return list(windows)
    if limit == 1:
        return [windows[0]]
    step = (len(windows) - 1) / (limit - 1)
    picked = {round(index * step) for index in range(limit)}
    return [windows[index] for index in sorted(picked)]


def _labels(values: Any, *, limit: int, max_chars: int) -> List[str]:
    out: List[str] = []
    seen = set()
    if not isinstance(values, (list, tuple)):
        return out
    for value in values:
        text = str(value or "").strip()
        if not text or len(text) > max_chars:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def parse_card_json(raw: str) -> Optional[Dict[str, Any]]:
    """모델 출력에서 JSON 객체만 건져낸다. 실패는 실패로 남긴다(추측 금지)."""
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            return None
        try:
            value = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return value if isinstance(value, dict) else None


def merge_window(card: DocumentCard, payload: Dict[str, Any]) -> bool:
    """윈도우 결과를 카드에 합친다. 사실은 누적, 요약은 교체. 새 사실이 있었는지 반환."""
    contributed = False
    title = str(payload.get("title") or "").strip()
    if title and not card.title:
        card.title = title[:200]
        contributed = True
    doc_type = str(payload.get("doc_type") or "").strip()
    if doc_type and not card.doc_type:
        card.doc_type = doc_type[:60]
        contributed = True
    language = str(payload.get("language") or "").strip()
    if language and card.language == "unknown":
        card.language = language[:20]
    summary = str(payload.get("summary") or "").strip()
    if summary:
        if summary != card.summary:
            contributed = contributed or not card.summary
        card.summary = summary
    for key, target, limit, max_chars in (
        ("topics", card.topics, MAX_LABELS, LABEL_MAX_CHARS),
        ("new_topics", card.topics, MAX_LABELS, LABEL_MAX_CHARS),
        ("entities", card.entities, MAX_LABELS, LABEL_MAX_CHARS),
        ("new_entities", card.entities, MAX_LABELS, LABEL_MAX_CHARS),
        ("keywords", card.keywords, MAX_LABELS, LABEL_MAX_CHARS),
        ("new_keywords", card.keywords, MAX_LABELS, LABEL_MAX_CHARS),
        ("answers_questions", card.answers_questions, MAX_QUESTIONS, QUESTION_MAX_CHARS),
        ("new_questions", card.answers_questions, MAX_QUESTIONS, QUESTION_MAX_CHARS),
    ):
        existing = {value.casefold() for value in target}
        for value in _labels(payload.get(key), limit=limit, max_chars=max_chars):
            if value.casefold() in existing or len(target) >= limit:
                continue
            target.append(value)
            existing.add(value.casefold())
            contributed = True
    return contributed


_CORE_RULES = """\
- 자료에 명시적으로 등장한 사실만 쓴다. 추측·일반 지식 보충 금지.
- 수치·단위·법령조항·고유명사는 글자 그대로 옮긴다.
- 없는 내용을 비워 두는 것이 지어내는 것보다 낫다.
- 모든 내용 필드는 문서의 언어를 그대로 쓴다.
"""

_JSON_SHAPE = """\
{"title": "문서 자체의 제목(파일명 아님)",
 "doc_type": "장르 명사 — 계약서/공고/회의록/명세서/보고서 등",
 "language": "ko/en 등",
 "summary": "이 문서가 무엇인지 2~3문장",
 "topics": ["이 문서가 다루는 대상 — 사업명·기관·주제"],
 "entities": ["기관·사람·제품·코드 등 고유명사"],
 "keywords": ["문서가 실제로 쓰는 어휘(동의어 브리지용)"],
 "answers_questions": ["이 문서가 답할 수 있는 질문 — 파일을 열지 말지 판단하는 사람을 위해"]}
"""


def build_first_prompt(file_name: str, window: Window) -> str:
    return (
        "당신은 문서를 카드로 정리하는 사서다. 아래는 문서의 일부다.\n"
        f"[파일명] {file_name}\n"
        f"[범위] 전체 {window.total}조각 중 {window.index + 1}번째\n\n"
        "이 조각만 보고 아래 JSON 을 채워라. 다른 조각은 나중에 따로 본다.\n"
        f"{_JSON_SHAPE}\n"
        f"규칙:\n{_CORE_RULES}"
        "- answers_questions 는 요약이 아니라 *검색 표면* 이다. 이 문서에만 있는 정보를 묻는 질문을 적어라.\n"
        "- JSON 객체만 출력한다. 코드펜스·설명 금지.\n\n"
        f"[자료]\n{window.text}\n\n[JSON]"
    )


def build_update_prompt(file_name: str, window: Window, card: DocumentCard) -> str:
    current = json.dumps(
        {
            "title": card.title,
            "doc_type": card.doc_type,
            "summary": card.summary,
            "topics": card.topics,
            "entities": card.entities,
            "keywords": card.keywords,
            "answers_questions": card.answers_questions,
        },
        ensure_ascii=False,
    )
    return (
        "같은 문서의 다음 조각이다. 지금까지의 카드를 갱신하라.\n"
        f"[파일명] {file_name}\n"
        f"[범위] 전체 {window.total}조각 중 {window.index + 1}번째\n\n"
        f"[현재 카드]\n{current}\n\n"
        "출력 JSON:\n"
        '{"summary": "이 조각까지 반영한 전체 요약 3~4문장(기존 요약에 덧붙이지 말고 새로 쓴다)",\n'
        ' "new_topics": [], "new_entities": [], "new_keywords": [], "new_questions": [],\n'
        ' "title": "카드의 제목이 틀렸을 때만", "doc_type": "카드의 종류가 틀렸을 때만"}\n\n'
        f"규칙:\n{_CORE_RULES}"
        "- new_* 는 카드에 아직 없는 것만 담는다. 이미 있는 항목을 반복하지 마라.\n"
        "- 이 조각이 참고문헌·상용구뿐이면 new_* 를 모두 비운다.\n"
        "- JSON 객체만 출력한다.\n\n"
        f"[자료]\n{window.text}\n\n[JSON]"
    )


async def _ask(prompt: str) -> Optional[Dict[str, Any]]:
    from app.services.llm import create_llm

    try:
        # temperature 를 넘기지 않는다 — config/llm_sampling.json 의 값을 쓴다.
        llm = create_llm(timeout=(15.0, 180.0), max_retries=2)
        response = await llm.ainvoke(prompt)
        raw = getattr(response, "content", response)
        return parse_card_json(raw if isinstance(raw, str) else str(raw))
    except Exception as exc:  # noqa: BLE001 - 한 조각 실패가 카드 전체를 막지 않는다
        logger.warning("[doc_cards] window LLM failed: %s", exc)
        return None


async def build_card(
    *,
    file_name: str,
    text: str,
    max_windows: int = MAX_WINDOWS,
    ask=_ask,
) -> DocumentCard:
    """문서 전문을 읽어 카드를 만든다. 예산을 넘으면 고르게 건너뛰고 그 사실을 남긴다."""
    windows = make_windows(text)
    card = DocumentCard()
    card.coverage.chars_total = len(text or "")
    card.coverage.windows_total = len(windows)
    if not windows:
        return card
    selected = evenly_spaced(windows, max_windows)
    if len(selected) < len(windows):
        logger.info(
            "[doc_cards] %s: %d조각 중 %d조각만 읽는다(예산 %d)",
            file_name, len(windows), len(selected), max_windows,
        )
    for position, window in enumerate(selected):
        prompt = (
            build_first_prompt(file_name, window)
            if position == 0 or not card.summary
            else build_update_prompt(file_name, window, card)
        )
        payload = await ask(prompt)
        card.coverage.windows_read += 1
        card.coverage.chars_read += len(window.text)
        if payload is None:
            card.coverage.windows_failed += 1
            continue
        if merge_window(card, payload):
            card.coverage.windows_contributed += 1
    if not card.title:
        card.title = file_name
    return card


def card_signature(*, text: str, model: str) -> str:
    digest = hashlib.sha256((text or "").encode("utf-8")).hexdigest()
    return f"v{CARD_VERSION}:{model}:{digest[:32]}"


def content_sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


async def load_existing_card(tenant_id: str, content_hash: str) -> Optional[Dict[str, Any]]:
    """같은 내용의 문서가 이미 카드가 있으면 재사용한다(중복 업로드 대비)."""
    from app.core.supabase_client import supabase

    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_doc_cards")
            .select("card, signature")
            .eq("tenant_id", tenant_id)
            .eq("content_sha256", content_hash)
            .eq("status", "done")
            .limit(1)
            .execute
        )
    except Exception as exc:  # noqa: BLE001 - 테이블 미배포 등
        logger.info("[doc_cards] 기존 카드 조회 불가: %s", exc)
        return None
    rows = getattr(result, "data", None) or []
    if not rows:
        return None
    card = rows[0].get("card")
    return card if isinstance(card, dict) else None


async def save_card(
    *,
    tenant_id: str,
    file_id: str,
    card: Dict[str, Any],
    signature: str,
    content_hash: str,
    status: str = "done",
) -> bool:
    """카드를 전용 테이블에 저장하고, 없으면 기존 doc_card 로 물러난다."""
    from app.core.supabase_client import supabase

    row = {
        "tenant_id": tenant_id,
        "file_id": file_id,
        "card": card,
        "signature": signature,
        "status": status,
        "content_sha256": content_hash,
    }
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_doc_cards").upsert(row, on_conflict="tenant_id,file_id").execute
        )
        return True
    except Exception as exc:  # noqa: BLE001
        logger.info("[doc_cards] knowledge_doc_cards 저장 실패(%s) — doc_card 로 폴백", exc)
    if status != "done" or not card.get("summary"):
        # 진행 상태(pending/failed)나 빈 카드를 doc_card 에 쓰면 기존 abstract 를 지운다.
        # 전용 테이블이 없는 배포에서는 완성된 카드만 폴백 저장한다.
        return False
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update({"doc_card": {
                "abstract": card.get("summary"),
                "abstract_status": "done" if card.get("summary") else "failed",
                "n_pages": (card.get("coverage") or {}).get("windows_total"),
                "card_version": CARD_VERSION,
            }})
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .execute
        )
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[doc_cards] doc_card 폴백 저장도 실패: %s", exc)
        return False


async def save_text_stats(*, tenant_id: str, file_id: str, text: str, page_count: int) -> None:
    """읽을 수 있는 문서인지 카탈로그가 알 수 있게 결정론 지표를 남긴다."""
    from app.core.supabase_client import supabase

    chars = len((text or "").strip())
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update({"has_text": chars > 0, "text_chars": chars, "page_count": page_count})
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .execute
        )
    except Exception as exc:  # noqa: BLE001 - 컬럼 미배포 시 조용히 건너뛴다
        logger.info("[doc_cards] text stats 저장 건너뜀: %s", exc)


__all__ = [
    "CARD_VERSION",
    "Coverage",
    "DocumentCard",
    "Window",
    "build_card",
    "build_first_prompt",
    "build_update_prompt",
    "card_signature",
    "content_sha256",
    "evenly_spaced",
    "load_existing_card",
    "make_windows",
    "merge_window",
    "parse_card_json",
    "save_card",
    "save_text_stats",
]
