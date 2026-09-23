"""document_blocks — 문서를 인용 앵커 단위(블록)로 저장한다.

블록은 문단·표(행 묶음)·그림 설명이다. 흐르는 문서(DOCX·HWPX)는 파서가 준 구조를 그대로
쓰고 쪽 번호가 없다. 쪽이 있는 문서(PDF·PPTX·XLSX)는 페이지 본문을 빈 줄로 나누고 쪽 번호를
단다. 계약: docs/specs/knowledge-map.md, 근거: docs/DESIGN_NOTES.md#흐르는-문서의-쪽-번호
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional

from langchain.schema import Document

logger = logging.getLogger(__name__)

# 파서가 Document.metadata 에 실어 보내는 블록 목록 키. 청크 메타데이터로 새지 않게 걸러낸다.
BLOCKS_KEY = "_blocks"
MAX_BLOCK_CHARS = 2000
_PAGE_HEADER = re.compile(r"^# 페이지 \d+$")


def _page_number(doc: Document) -> Optional[int]:
    """쪽 개념이 있는 문서만 1-based 쪽 번호. page 는 0-based, page_number 는 1-based."""
    meta = doc.metadata or {}
    for key, offset in (("page_number", 0), ("page", 1)):
        value = meta.get(key)
        if value is not None:
            try:
                return int(value) + offset
            except (TypeError, ValueError):
                return None
    return None


def _split_table(text: str) -> List[str]:
    lines = text.splitlines()
    header = lines[:2] if len(lines) > 2 and set(lines[1].replace("|", "").strip()) <= set("-: ") else []
    rows = lines[len(header):]
    parts: List[str] = []
    current: List[str] = []
    size = sum(len(line) + 1 for line in header)
    for row in rows:
        if current and size + len(row) + 1 > MAX_BLOCK_CHARS:
            parts.append("\n".join(header + current))
            current, size = [], sum(len(line) + 1 for line in header)
        current.append(row)
        size += len(row) + 1
    if current:
        parts.append("\n".join(header + current))
    return parts


def _pieces(line: str) -> List[str]:
    """한 줄이 한도를 넘으면 공백 경계에서, 공백이 없으면 글자 수로 자른다."""
    if len(line) <= MAX_BLOCK_CHARS:
        return [line]
    pieces: List[str] = []
    while len(line) > MAX_BLOCK_CHARS:
        cut = line.rfind(" ", 0, MAX_BLOCK_CHARS)
        cut = cut if cut > 0 else MAX_BLOCK_CHARS
        pieces.append(line[:cut].rstrip())
        line = line[cut:].lstrip()
    if line:
        pieces.append(line)
    return pieces


def _split_text(text: str) -> List[str]:
    parts: List[str] = []
    current = ""
    for line in (piece for raw in text.splitlines() for piece in _pieces(raw)):
        if current and len(current) + len(line) + 1 > MAX_BLOCK_CHARS:
            parts.append(current)
            current = ""
        current = f"{current}\n{line}" if current else line
    if current:
        parts.append(current)
    return parts


def _sized(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    """한 블록이 너무 크면 줄·표 행 경계에서 나눈다. 헤딩은 나누지 않는다."""
    text = block["text"]
    if len(text) <= MAX_BLOCK_CHARS or block.get("heading_level"):
        return [block]
    pieces = _split_table(text) if block["kind"] == "table" else _split_text(text)
    return [{**block, "text": piece} for piece in pieces]


def _page_segments(doc: Document) -> List[Dict[str, Any]]:
    """쪽 본문을 빈 줄로 나눈 블록. PDF 는 blocks_json 의 offset 으로 bbox 를 붙인다."""
    content = doc.page_content or ""
    page = _page_number(doc)
    bboxes: Dict[int, Any] = {}
    raw = (doc.metadata or {}).get("blocks_json")
    if raw:
        try:
            bboxes = {int(b["offset"]): b.get("bbox") for b in json.loads(raw)}
        except (TypeError, ValueError, KeyError):
            bboxes = {}
    segments: List[Dict[str, Any]] = []
    for match in re.finditer(r"\S(?:.*?)(?=\n\s*\n|\Z)", content, re.S):
        text = match.group(0).strip()
        if not text or _PAGE_HEADER.match(text):
            continue
        kind = "table" if text.startswith("|") else "paragraph"
        segments.append({
            "kind": kind, "text": text, "heading_level": None,
            "page_number": page, "bbox": bboxes.get(match.start()),
        })
    return segments


def build_blocks(page_docs: List[Document]) -> List[Dict[str, Any]]:
    """load_document() 결과 → 문서 순서의 블록 목록(block_index 부여)."""
    blocks: List[Dict[str, Any]] = []
    for doc in page_docs:
        given = (doc.metadata or {}).get(BLOCKS_KEY)
        if given:
            items = [
                {"kind": b["kind"], "text": (b.get("text") or "").strip(),
                 "heading_level": b.get("heading_level"), "page_number": None, "bbox": None}
                for b in given
            ]
        else:
            items = _page_segments(doc)
        for item in items:
            if item["text"]:
                blocks.extend(_sized(item))
    for index, block in enumerate(blocks):
        block["block_index"] = index
    return blocks


async def save_blocks(tenant_id: str, file_id: str, blocks: List[Dict[str, Any]]) -> int:
    """document_blocks 를 파일 단위로 교체한다. 실패는 격리(0 반환) — 인제스트를 막지 않는다."""
    from app.core.supabase_client import supabase

    if not tenant_id or not file_id:
        return 0
    try:
        await asyncio.to_thread(
            supabase.table("document_blocks").delete()
            .eq("tenant_id", tenant_id).eq("file_id", file_id).execute
        )
        rows = [
            {
                "tenant_id": tenant_id,
                "file_id": file_id,
                "block_index": b["block_index"],
                "kind": b["kind"],
                "text": b["text"].replace("\x00", ""),
                "heading_level": b["heading_level"],
                "page_number": b["page_number"],
                "bbox": b["bbox"],
            }
            for b in blocks
        ]
        for start in range(0, len(rows), 500):
            await asyncio.to_thread(
                supabase.table("document_blocks").insert(rows[start:start + 500]).execute
            )
        return len(rows)
    except Exception as exc:  # noqa: BLE001 - 블록은 페이지 뒤의 추가 층이다
        logger.warning("[document_blocks] save failed (%s/%s): %s", tenant_id, file_id, exc)
        return 0
