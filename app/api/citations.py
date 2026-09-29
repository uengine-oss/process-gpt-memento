"""인용 뷰어 API — 문서 블록 전체, PDF 쪽 이미지, 인용 문장 → 블록 범위.

인용 앵커는 블록이다. 쪽 번호·bbox 는 쪽이 있는 형식의 블록에만 붙는다.
계약: docs/specs/knowledge-map.md#인용-뷰어-api
"""
from __future__ import annotations

import asyncio
import logging
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from app.api.sections import _file_ref
from app.core.supabase_client import supabase
from app.services import rendition
from app.storage.artifact_bucket import bucket_for

router = APIRouter()
logger = logging.getLogger(__name__)

_PAGE_ROWS = 1000
_PDF_CACHE: "OrderedDict[str, bytes]" = OrderedDict()
_PDF_CACHE_MAX = 8


async def _all_blocks(tenant_id: str, file_id: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    while True:
        result = await asyncio.to_thread(
            supabase.table("document_blocks")
            .select("block_index, kind, text, heading_level, page_number, bbox")
            .eq("tenant_id", tenant_id).eq("file_id", file_id)
            .order("block_index").range(len(rows), len(rows) + _PAGE_ROWS - 1).execute
        )
        batch = result.data or []
        rows.extend(batch)
        if len(batch) < _PAGE_ROWS:
            return rows


async def _sections(tenant_id: str, file_id: str) -> List[Dict[str, Any]]:
    result = await asyncio.to_thread(
        supabase.table("document_sections").select("section_index, start_block, end_block, title")
        .eq("tenant_id", tenant_id).eq("file_id", file_id).order("section_index").execute
    )
    return result.data or []


async def _file_row(tenant_id: str, file_id: str) -> Dict[str, Any]:
    result = await asyncio.to_thread(
        supabase.table("knowledge_files").select("source_ref, file_name, path, mime_type, page_count, file_hash")
        .eq("tenant_id", tenant_id).eq("source_ref", file_id).limit(1).execute
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="file not found")
    return result.data[0]


def _section_of(sections: List[Dict[str, Any]], block_index: int) -> Optional[str]:
    for s in sections:
        if s["start_block"] <= block_index <= s["end_block"]:
            return s["title"]
    return None


async def _rendition(meta: Dict[str, Any], blocks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    ref = meta["source_ref"]
    return await rendition.ensure_rendition(
        file_id=ref, file_name=meta.get("file_name") or ref, file_hash=meta.get("file_hash") or "",
        blocks=blocks, load_bytes=lambda: _original_bytes(ref),
    )


@router.get("/document/blocks")
async def document_blocks(
    tenant_id: str, file_id: Optional[str] = None, path: Optional[str] = None, render: bool = True,
):
    """문서 전체 블록과 섹션. 블록마다 칠할 자리(rects: 쪽·bbox)를 붙인다.

    PDF는 원본 쪽(page_basis=original). 흐르는 문서는 변환본을 그려 그 위 자리를 붙인다
    (page_basis=rendition) — 첫 요청은 변환 시간이 든다. 렌더러가 없으면 rects 없이 flowing.
    """
    ref = await _file_ref(tenant_id, file_id, path)
    meta, blocks, sections = await asyncio.gather(
        _file_row(tenant_id, ref), _all_blocks(tenant_id, ref), _sections(tenant_id, ref))
    paged = any(b.get("page_number") for b in blocks)
    out = {
        "file_id": ref,
        "file_name": meta.get("file_name"),
        "path": meta.get("path"),
        "mime_type": meta.get("mime_type"),
        "layout": "flowing",
        "page_basis": None,
        "page_count": None,
        "blocks": blocks,
        "sections": sections,
    }
    if paged:
        for b in blocks:
            b["rects"] = [{"page": b["page_number"], "bbox": b["bbox"]}] if b.get("bbox") else []
        out.update(layout="paged", page_basis="original", page_count=meta.get("page_count"))
        return out
    if not render:
        return out
    try:
        rend = await _rendition(meta, blocks)
    except Exception as exc:  # noqa: BLE001 - 변환 실패는 흐르는 보기로 물러난다
        logger.warning("[/document/blocks] rendition failed (%s): %s", ref, exc)
        out["render_error"] = str(exc)[:300]
        return out
    if rend:
        placed = rend["placed"]
        for b in blocks:
            b["rects"] = (placed.get(str(b["block_index"])) or {}).get("rects") or []
        out.update(layout="paged", page_basis="rendition", page_count=rend["page_count"],
                   renderer=rend["renderer"], coverage=rend["coverage"])
    return out


async def _original_bytes(file_id: str) -> bytes:
    return await asyncio.to_thread(supabase.storage.from_(bucket_for(file_id)).download, file_id)


async def _pdf_bytes(tenant_id: str, file_id: str) -> bytes:
    if file_id in _PDF_CACHE:
        _PDF_CACHE.move_to_end(file_id)
        return _PDF_CACHE[file_id]
    if file_id.lower().endswith(".pdf"):
        data = await _original_bytes(file_id)
    else:
        meta, blocks = await asyncio.gather(_file_row(tenant_id, file_id), _all_blocks(tenant_id, file_id))
        rend = await _rendition(meta, blocks)
        if not rend:
            raise HTTPException(status_code=415, detail="no renderer for this format")
        data = await asyncio.to_thread(Path(rend["pdf_path"]).read_bytes)
    _PDF_CACHE[file_id] = data
    while len(_PDF_CACHE) > _PDF_CACHE_MAX:
        _PDF_CACHE.popitem(last=False)
    return data


@router.get("/document/page-image")
async def document_page_image(
    tenant_id: str,
    page: int = Query(ge=1),
    file_id: Optional[str] = None,
    path: Optional[str] = None,
    scale: float = Query(default=1.5, gt=0.2, le=4.0),
):
    """PDF(흐르는 문서는 변환본) 한 쪽을 PNG 로. bbox 는 PDF 포인트 단위라 X-Page-Width/Height 로 비율을 맞춘다."""
    import fitz  # PyMuPDF

    ref = await _file_ref(tenant_id, file_id, path)
    data = await _pdf_bytes(tenant_id, ref)

    def render() -> tuple[bytes, float, float]:
        with fitz.open(stream=data, filetype="pdf") as pdf:
            if page > pdf.page_count:
                raise HTTPException(status_code=404, detail="page out of range")
            p = pdf[page - 1]
            png = p.get_pixmap(matrix=fitz.Matrix(scale, scale)).tobytes("png")
            return png, p.rect.width, p.rect.height

    png, width, height = await asyncio.to_thread(render)
    return Response(content=png, media_type="image/png", headers={
        "X-Page-Width": f"{width:.2f}", "X-Page-Height": f"{height:.2f}",
        "Access-Control-Expose-Headers": "X-Page-Width, X-Page-Height",
        "Cache-Control": "private, max-age=3600",
    })


def _squash(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


# 발췌를 줄인 자리("...", "…"). 조각들이 순서대로 가까이 나오면 한 인용으로 본다.
_ELLIPSIS = re.compile(r"\.{3,}|…+")
_ELLIPSIS_GAP = 400


def _quote_pattern(quote: str) -> str:
    pieces = [re.escape(p) for p in (_squash(x) for x in _ELLIPSIS.split(quote or "")) if p]
    return f".{{0,{_ELLIPSIS_GAP}}}?".join(pieces)


@router.get("/document/locate")
async def document_locate(tenant_id: str, quote: str, file_id: Optional[str] = None, path: Optional[str] = None):
    """인용 문장이 걸친 블록 범위. 공백을 무시하고 맞추며, 여러 곳이면 모두 돌려준다."""
    needle = _quote_pattern(quote)
    if not needle:
        raise HTTPException(status_code=400, detail="quote required")
    ref = await _file_ref(tenant_id, file_id, path)
    blocks, sections = await asyncio.gather(_all_blocks(tenant_id, ref), _sections(tenant_id, ref))
    joined, owner = [], []
    for i, b in enumerate(blocks):
        s = _squash(b["text"])
        joined.append(s)
        owner.extend([i] * len(s))
    haystack = "".join(joined)
    matches = []
    for m in re.finditer(needle, haystack):
        first, last = blocks[owner[m.start()]], blocks[owner[m.end() - 1]]
        pages = sorted({b["page_number"] for b in blocks[owner[m.start()]:owner[m.end() - 1] + 1] if b.get("page_number")})
        matches.append({
            "start_block": first["block_index"],
            "end_block": last["block_index"],
            "pages": pages or None,
            "section": _section_of(sections, first["block_index"]),
        })
    return {"file_id": ref, "quote": quote, "matches": matches}
