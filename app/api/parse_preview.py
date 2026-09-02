"""파싱 미리보기 라우터: /parse/preview, /parse/stored.

목적 — "문서가 파서를 거쳐 어떤 텍스트(마크다운)로 변환됐나"를 눈으로 확인.

- POST /parse/preview : 파일을 올리면 *저장 없이* 파서만 돌려 페이지 본문(+선택적 청크)을 반환.
    지식베이스(Supabase/Chroma)를 전혀 건드리지 않는 dry-run. 파서 설정 튜닝/품질 확인용.
- GET  /parse/stored  : 이미 인덱싱된 파일의 document_pages 본문을 source_ref 로 그대로 반환.
    실제 저장된 파싱 결과(= RAG 가 읽는 페이지 본문)를 미리보기.
"""
from __future__ import annotations

import asyncio
import io
import logging
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

from app.core.supabase_client import supabase
from app.services.document_processor import get_document_processor

logger = logging.getLogger(__name__)
router = APIRouter()


def _page_no(meta: dict, fallback: int) -> int:
    """페이지 번호 추출 — 파서마다 page_number(1-based) 또는 page(0-based)를 씀. 없으면 순번."""
    pn = meta.get("page_number")
    if isinstance(pn, int):
        return pn
    p = meta.get("page")
    if isinstance(p, int):
        return p + 1  # page 는 0-based → 표시용 1-based
    return fallback


@router.post("/parse/preview")
async def parse_preview(
    file: UploadFile = File(...),
    tenant_id: str = Form(""),
    include_chunks: bool = Form(False),
):
    """업로드된 파일을 저장 없이 파싱만 해서 페이지 본문을 반환.

    Args:
        file: 파싱할 문서 (pdf/docx/pptx/hwp/hwpx/xlsx/txt ...).
        tenant_id: 청킹 메타 표시용(선택). 저장하지 않으므로 필수 아님.
        include_chunks: true 면 process_documents 로 실제 청킹 결과도 함께 반환.

    Returns:
        {file_name, page_count, pages: [{page_number, content}], (chunks?), (chunk_count?)}
    """
    filename = file.filename or "uploaded"
    try:
        content = await file.read()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"파일 읽기 실패: {exc}")
    if not content:
        raise HTTPException(status_code=400, detail="빈 파일")

    processor = get_document_processor()
    try:
        page_docs = await processor.load_document(io.BytesIO(content), filename)
    except Exception as exc:  # noqa: BLE001 — 파싱 실패도 미리보기에선 사용자에게 그대로 노출
        logger.exception("[/parse/preview] 파싱 실패 file=%s", filename)
        raise HTTPException(status_code=422, detail=f"파싱 실패: {exc}")

    if not page_docs:
        raise HTTPException(status_code=422, detail="본문 추출 실패 (빈 본문 또는 지원하지 않는 형식)")

    pages = [
        {"page_number": _page_no(d.metadata or {}, i + 1), "content": d.page_content or ""}
        for i, d in enumerate(page_docs)
    ]
    result: dict = {"file_name": filename, "page_count": len(pages), "pages": pages}

    if include_chunks:
        try:
            chunk_docs = await processor.process_documents(
                page_docs,
                {
                    "tenant_id": tenant_id or "preview",
                    "original_filename": filename,
                    "storage_type": "preview",
                },
            )
            result["chunks"] = [
                {
                    "index": i,
                    "content": c.page_content or "",
                    "page_number": _page_no(c.metadata or {}, i + 1),
                    "section_title": (c.metadata or {}).get("section_title") or "",
                }
                for i, c in enumerate(chunk_docs or [])
            ]
            result["chunk_count"] = len(result["chunks"])
        except Exception as exc:  # noqa: BLE001 — 청킹 실패해도 페이지 미리보기는 살림
            logger.warning("[/parse/preview] 청킹 실패(계속): %s", exc)
            result["chunk_error"] = str(exc)

    logger.info(
        "[/parse/preview] file=%s pages=%d chunks=%s",
        filename, len(pages), result.get("chunk_count"),
    )
    return result


@router.get("/parse/stored")
async def parse_stored(
    tenant_id: str = Query(...),
    source_ref: str = Query(..., description="knowledge_files.source_ref (= document_pages.file_id)"),
):
    """이미 인덱싱된 파일의 document_pages 본문을 전 페이지 반환 (실제 저장된 파싱 결과).

    Returns:
        {source_ref, page_count, pages: [{page_number, content}]}
    """
    if not tenant_id or not source_ref:
        raise HTTPException(status_code=400, detail="tenant_id, source_ref required")

    try:
        resp = await asyncio.to_thread(
            supabase.table("document_pages")
            .select("page_number, content")
            .eq("tenant_id", tenant_id)
            .eq("file_id", source_ref)
            .order("page_number", desc=False)
            .execute
        )
        rows = resp.data or []
    except Exception as exc:  # noqa: BLE001
        logger.exception("[/parse/stored] query failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    pages = [
        {"page_number": r.get("page_number"), "content": r.get("content") or ""}
        for r in rows
    ]
    logger.info("[/parse/stored] tenant=%s ref=%s → pages=%d", tenant_id, source_ref, len(pages))
    return {"source_ref": source_ref, "page_count": len(pages), "pages": pages}
