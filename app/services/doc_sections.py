"""문서 섹션 — 블록 위에 얹는 탐색 단위(목차).

섹션 시작은 카드 LLM 이 창마다 고른다(doc_cards.build_card). 여기서는 그 결과를 정리해
문서 전체를 빈틈없이 덮는 목차로 만들고, 너무 큰 섹션은 한 번 더 나눈다.
인용의 정확성은 블록이 지고 섹션은 길찾기만 한다. 근거: docs/DESIGN_NOTES.md#섹션
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# 검색·재순위 단위로 쓰기에 너무 큰 섹션은 다시 나눈다(표 하나짜리 붙임이 48,842자였다).
SECTION_MAX_CHARS = int(os.getenv("KB_SECTION_MAX_CHARS", "8000"))
PREVIEW_CHARS = 200
FRONT_TITLE = "(앞부분)"

Ask = Callable[[str], Awaitable[Optional[Dict[str, Any]]]]

SUBDIVIDE_PROMPT = """아래는 문서 "{file_name}"의 섹션 "{title}" 이다. 너무 길어서 목차로 쓰기 어렵다.
각 줄은 [블록ID] 블록 앞부분이다. 이 섹션을 하위 항목으로 나눌 시작 블록을 골라라.
- 표라면 행 묶음(예: 목표 번호, 분류)으로, 본문이라면 소제목·번호로 나눈다.
- title 은 문서에 쓰인 말을 쓰고, 없으면 짧은 명사구로 요약한다.
- 첫 블록 [b{first}] 는 반드시 첫 하위 항목의 시작이다.

JSON만 출력: {{"sections": [{{"start": "b{first}", "title": "하위 항목 제목", "summary": "한 문장"}}]}}

[블록]
{lines}
"""


def _chars(blocks: Sequence[Dict[str, Any]], start: int, end: int) -> int:
    return sum(len(blocks[i]["text"]) for i in range(start, end + 1))


def normalize(raw: Sequence[Dict[str, Any]], blocks: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """모델이 고른 시작점 → 문서 전체를 덮는 연속 섹션. 같은 시작·같은 제목의 반복은 하나로 친다."""
    if not blocks:
        return []
    by_start: Dict[int, Dict[str, Any]] = {}
    for item in raw:
        start = int(item["start"])
        if 0 <= start < len(blocks) and start not in by_start:
            by_start[start] = {"start": start, "title": item["title"], "summary": item.get("summary", ""),
                               "source": item.get("source", "llm")}
    ordered: List[Dict[str, Any]] = []
    for section in sorted(by_start.values(), key=lambda s: s["start"]):
        if ordered and ordered[-1]["title"] == section["title"]:
            continue
        ordered.append(section)
    if not ordered or ordered[0]["start"] != 0:
        ordered.insert(0, {"start": 0, "title": FRONT_TITLE, "summary": "", "source": "llm"})
    for current, following in zip(ordered, ordered[1:] + [{"start": len(blocks)}]):
        current["end"] = following["start"] - 1
        current["chars"] = _chars(blocks, current["start"], current["end"])
    return ordered


def _lead(block: Dict[str, Any]) -> str:
    """잘린 조각의 제목에 붙일 첫 내용 — 표는 반복되는 머리행 대신 첫 데이터 행."""
    lines = [line for line in block["text"].splitlines() if line.strip()]
    if block.get("kind") == "table" and len(lines) > 2 and set(lines[1].replace("|", "").strip()) <= set("-: "):
        lines = lines[2:]
    # 비공개 처리(****)된 행처럼 글자가 없는 줄은 건너뛴다.
    readable = [line for line in lines if len(re.findall(r"[0-9A-Za-z가-힣]", line)) >= 4]
    text = " ".join((readable[0] if readable else "").replace("|", " ").split())
    return text[:40]


def _size_split(section: Dict[str, Any], blocks: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parts: List[Dict[str, Any]] = []
    start, size = section["start"], 0
    for index in range(section["start"], section["end"] + 1):
        length = len(blocks[index]["text"])
        if index > start and size + length > SECTION_MAX_CHARS:
            parts.append({"start": start, "end": index - 1})
            start, size = index, 0
        size += length
    parts.append({"start": start, "end": section["end"]})
    return [
        {**part,
         "title": section["title"] if n == 0 else f"{section['title']} (계속 {n}: {_lead(blocks[part['start']])})"[:120],
         "summary": section["summary"] if n == 0 else "", "source": "split",
         "chars": _chars(blocks, part["start"], part["end"])}
        for n, part in enumerate(parts)
    ]


async def _subdivide(
    section: Dict[str, Any], blocks: Sequence[Dict[str, Any]], file_name: str, ask: Ask
) -> List[Dict[str, Any]]:
    """큰 섹션 하나를 LLM 으로 한 번 더 나눈다. 못 나누면 블록 경계에서 크기로 자른다."""
    ids = range(section["start"], section["end"] + 1)
    lines = "\n".join(f"[b{i}] " + " ".join(blocks[i]["text"][:PREVIEW_CHARS].split()) for i in ids)
    payload = await ask(SUBDIVIDE_PROMPT.format(
        file_name=file_name, title=section["title"], first=section["start"], lines=lines,
    ))
    starts: Dict[int, Dict[str, Any]] = {}
    for item in (payload or {}).get("sections") or []:
        start = str((item or {}).get("start") or "").strip().lstrip("bB")
        title = " ".join(str((item or {}).get("title") or "").split())
        if start.isdigit() and section["start"] <= int(start) <= section["end"] and title:
            starts.setdefault(int(start), {
                "start": int(start), "title": f"{section['title']} › {title}"[:120],
                "summary": " ".join(str(item.get("summary") or "").split())[:300], "source": "llm",
            })
    if len(starts) < 2:
        return _size_split(section, blocks)
    starts.setdefault(section["start"], {"start": section["start"], "title": section["title"],
                                         "summary": section["summary"], "source": "llm"})
    children = sorted(starts.values(), key=lambda c: c["start"])
    out: List[Dict[str, Any]] = []
    for child, following in zip(children, children[1:] + [{"start": section["end"] + 1}]):
        child["end"] = following["start"] - 1
        child["chars"] = _chars(blocks, child["start"], child["end"])
        out.extend(_size_split(child, blocks) if child["chars"] > SECTION_MAX_CHARS else [child])
    return out


async def finalize(
    raw: Sequence[Dict[str, Any]], blocks: Sequence[Dict[str, Any]], file_name: str, ask: Ask
) -> List[Dict[str, Any]]:
    sections: List[Dict[str, Any]] = []
    for section in normalize(raw, blocks):
        if section["chars"] > SECTION_MAX_CHARS and section["end"] > section["start"]:
            sections.extend(await _subdivide(section, blocks, file_name, ask))
        else:
            sections.append(section)
    for index, section in enumerate(sections):
        section["section_index"] = index
    return sections


async def load_blocks(tenant_id: str, file_id: str) -> List[Dict[str, Any]]:
    from app.core.supabase_client import supabase

    rows: List[Dict[str, Any]] = []
    offset = 0
    try:
        while True:
            result = await asyncio.to_thread(
                supabase.table("document_blocks")
                .select("block_index, kind, text, heading_level, page_number")
                .eq("tenant_id", tenant_id).eq("file_id", file_id)
                .order("block_index").range(offset, offset + 999).execute
            )
            batch = getattr(result, "data", None) or []
            rows.extend(batch)
            if len(batch) < 1000:
                return rows
            offset += 1000
    except Exception as exc:  # noqa: BLE001 - 테이블 미배포 시 페이지에서 블록을 만든다
        logger.info("[doc_sections] blocks not loaded (%s/%s): %s", tenant_id, file_id, exc)
        return []


async def save_sections(tenant_id: str, file_id: str, sections: Sequence[Dict[str, Any]]) -> int:
    from app.core.supabase_client import supabase

    try:
        await asyncio.to_thread(
            supabase.table("document_sections").delete()
            .eq("tenant_id", tenant_id).eq("file_id", file_id).execute
        )
        rows = [
            {"tenant_id": tenant_id, "file_id": file_id, "section_index": s["section_index"],
             "start_block": s["start"], "end_block": s["end"], "title": s["title"],
             "summary": s.get("summary") or "", "chars": s["chars"], "source": s.get("source", "llm")}
            for s in sections
        ]
        for start in range(0, len(rows), 500):
            await asyncio.to_thread(supabase.table("document_sections").insert(rows[start:start + 500]).execute)
        return len(rows)
    except Exception as exc:  # noqa: BLE001 - 섹션은 카드 뒤의 추가 층이다
        logger.warning("[doc_sections] save failed (%s/%s): %s", tenant_id, file_id, exc)
        return 0


async def copy_sections(tenant_id: str, source_file_id: str, file_id: str) -> int:
    """같은 내용의 문서면 블록 번호도 같다 — 섹션을 그대로 복사한다."""
    from app.core.supabase_client import supabase

    try:
        result = await asyncio.to_thread(
            supabase.table("document_sections").select("*")
            .eq("tenant_id", tenant_id).eq("file_id", source_file_id).order("section_index").execute
        )
    except Exception as exc:  # noqa: BLE001
        logger.info("[doc_sections] copy skipped: %s", exc)
        return 0
    sections = [
        {"section_index": r["section_index"], "start": r["start_block"], "end": r["end_block"],
         "title": r["title"], "summary": r.get("summary") or "", "chars": r["chars"], "source": r.get("source")}
        for r in (getattr(result, "data", None) or [])
    ]
    return await save_sections(tenant_id, file_id, sections) if sections else 0
