"""섹션 API — 섹션 검색, 문서 목차, 섹션 본문. 계약: docs/specs/knowledge-map.md#섹션-api"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.api.navigator import _resolve_file_id
from app.api.retrieve import _resolve_subtree_file_ids
from app.core.supabase_client import supabase
from app.services import section_search

router = APIRouter()
logger = logging.getLogger(__name__)

MAX_SECTION_TEXT = 20000


async def _scope(tenant_id: str, file_ids: List[str], folder_paths: List[str]) -> List[str]:
    """폴더 subtree ∪ file_ids. 둘 다 비면 테넌트 전체."""
    ids = set(file_ids)
    if folder_paths:
        ids |= set(await _resolve_subtree_file_ids(tenant_id, folder_paths))
    if ids or folder_paths:
        return sorted(ids)
    result = await asyncio.to_thread(
        supabase.table("knowledge_files").select("source_ref").eq("tenant_id", tenant_id).limit(20000).execute
    )
    return sorted({r["source_ref"] for r in (result.data or []) if r.get("source_ref")})


async def _files(tenant_id: str, file_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(file_ids), 100):
        result = await asyncio.to_thread(
            supabase.table("knowledge_files").select("source_ref, file_name, folder_path, path")
            .eq("tenant_id", tenant_id).in_("source_ref", file_ids[start:start + 100]).execute
        )
        for row in result.data or []:
            out[row["source_ref"]] = row
    return out


async def _file_ref(tenant_id: str, file_id: Optional[str], path: Optional[str]) -> str:
    ref = (file_id or "").strip() or await _resolve_file_id(tenant_id, path=path)
    if not ref:
        raise HTTPException(status_code=404, detail="file not found")
    return ref


@router.get("/sections/search")
async def search_sections(
    tenant_id: str,
    query: str,
    file_ids: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
    top_k: int = Query(default=20, ge=1, le=100),
):
    """키워드(부분 일치, BM25 모양)와 섹션 벡터를 RRF 로 합친 섹션 순위."""
    if not query.strip():
        raise HTTPException(status_code=400, detail="query required")
    scope = await _scope(tenant_id, [f for f in (file_ids or []) if f], [p for p in (folder_paths or []) if p])
    if not scope:
        return {"query": query, "terms": [], "results": [], "errors": {}}
    found = await section_search.search(tenant_id, scope, query, top_k)
    keys = [(r["file_id"], r["section_index"]) for r in found["results"]]
    sections = await section_search.load_sections(tenant_id, keys)
    files = await _files(tenant_id, sorted({k[0] for k in keys}))
    results = []
    for item in found["results"]:
        key = (item["file_id"], item["section_index"])
        section = sections.get(key)
        if not section:
            continue
        blocks = await section_search.load_block_range(
            tenant_id, key[0], section["start_block"], section["end_block"])
        text = "\n".join(b["text"] for b in blocks)
        meta = files.get(key[0], {})
        results.append({
            "file_id": key[0],
            "file_name": meta.get("file_name"),
            "path": meta.get("path") or meta.get("file_name"),
            "section_index": key[1],
            "title": section["title"],
            "summary": section.get("summary") or "",
            "start_block": section["start_block"],
            "end_block": section["end_block"],
            "chars": section["chars"],
            "pages": section_search.pages_of(blocks),
            "score": round(item["score"], 5),
            "ranks": item["ranks"],
            "matched_terms": item["matched_terms"],
            "snippet": section_search.snippet(text, item["matched_terms"] or found["terms"]),
        })
    logger.info("[/sections/search] tenant=%s scope=%d q=%r → %d (errors=%s)",
                tenant_id, len(scope), query[:60], len(results), found["errors"] or None)
    return {"query": query, "terms": found["terms"], "results": results, "errors": found["errors"]}


@router.get("/document/outline")
async def document_outline(
    tenant_id: str, file_id: Optional[str] = None, path: Optional[str] = None,
):
    """문서의 섹션 목차. 섹션이 아직 없으면 빈 목록과 함께 그 사실을 돌려준다."""
    ref = await _file_ref(tenant_id, file_id, path)
    result = await asyncio.to_thread(
        supabase.table("document_sections").select("section_index, start_block, end_block, title, summary, chars, source")
        .eq("tenant_id", tenant_id).eq("file_id", ref).order("section_index").execute
    )
    rows = result.data or []
    return {"file_id": ref, "ready": bool(rows), "sections": rows}


@router.get("/documents/outlines")
async def documents_outlines(
    tenant_id: str,
    file_ids: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
):
    """선택 범위 전체의 섹션 목차를 파일별로 한 번에. 미러를 만들 때 쓴다."""
    scope = await _scope(tenant_id, [f for f in (file_ids or []) if f], [p for p in (folder_paths or []) if p])
    outlines: Dict[str, List[Dict[str, Any]]] = {}
    for start in range(0, len(scope), 100):
        offset = 0
        while True:
            result = await asyncio.to_thread(
                supabase.table("document_sections")
                .select("file_id, section_index, start_block, end_block, title, summary, chars, source")
                .eq("tenant_id", tenant_id).in_("file_id", scope[start:start + 100])
                .order("file_id").order("section_index").range(offset, offset + 999).execute
            )
            rows = result.data or []
            for row in rows:
                outlines.setdefault(row.pop("file_id"), []).append(row)
            if len(rows) < 1000:
                break
            offset += 1000
    return {"outlines": outlines}


@router.get("/document/section")
async def document_section(
    tenant_id: str,
    file_id: Optional[str] = None,
    path: Optional[str] = None,
    section_index: Optional[int] = None,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
):
    """섹션(또는 블록 범위) 본문. 블록마다 [bN] 앵커를 달아 인용 위치를 남긴다."""
    ref = await _file_ref(tenant_id, file_id, path)
    title = ""
    if section_index is not None:
        found = await section_search.load_sections(tenant_id, [(ref, section_index)])
        section = found.get((ref, section_index))
        if not section:
            raise HTTPException(status_code=404, detail="section not found")
        start_block, end_block, title = section["start_block"], section["end_block"], section["title"]
    if start_block is None or end_block is None or end_block < start_block:
        raise HTTPException(status_code=400, detail="section_index or start_block/end_block required")
    blocks = await section_search.load_block_range(tenant_id, ref, start_block, end_block)
    lines: List[str] = []
    size = 0
    truncated = False
    for block in blocks:
        line = f"[b{block['block_index']}] {block['text']}"
        if size + len(line) > MAX_SECTION_TEXT and lines:
            truncated = True
            break
        lines.append(line)
        size += len(line) + 1
    return {
        "file_id": ref, "section_index": section_index, "title": title,
        "start_block": start_block, "end_block": end_block,
        "pages": section_search.pages_of(blocks), "truncated": truncated, "text": "\n".join(lines),
    }
