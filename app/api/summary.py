"""Summary 라우터 — 문서 요약 전용 엔드포인트.

deep-agents-temp 의 summary-pipeline 서브에이전트가 호출.
LLM 무관 코드 흐름으로 page batch 분할 + 병렬 mini-summary 생성.
agent loop 부담 없이 *도구 1번 호출 = 전체 batch 결과* 받음.

엔드포인트:
    GET /summarize  → 문서의 batch 별 mini-summary 리스트

매칭 키: ``file_name`` 만 LLM 에 노출 (file_id = source_ref 는 내부 해석).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from app.core.supabase_client import supabase
from app.services.summary_service import (
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    MAX_PARALLEL_LLM_CALLS,
    MIN_BATCH_SIZE,
    summarize_document as svc_summarize_document,
)


router = APIRouter()
logger = logging.getLogger(__name__)


async def _resolve_file_id(tenant_id: str, file_name: str) -> Optional[str]:
    """동일 tenant 안에서 file_name 매칭되는 첫 knowledge_files row 의 source_ref 반환."""
    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref, modified_time")
            .eq("tenant_id", tenant_id)
            .eq("file_name", file_name)
            .order("modified_time", desc=True)
            .limit(1)
            .execute
        )
        rows = result.data or []
        if not rows:
            return None
        return rows[0].get("source_ref")
    except Exception as e:
        logger.warning(
            "[summary] resolve file_id failed (tenant=%s, name=%s): %s",
            tenant_id, file_name, e,
        )
        return None


@router.get("/summarize")
async def summarize(
    tenant_id: str,
    file_name: str,
    batch_size: int = Query(default=DEFAULT_BATCH_SIZE, ge=MIN_BATCH_SIZE, le=MAX_BATCH_SIZE),
    parallelism: int = Query(default=MAX_PARALLEL_LLM_CALLS, ge=1, le=16),
    force_refresh: bool = Query(default=False),
):
    """문서 1개를 page batch 단위로 병렬 mini-summary.

    Args:
        tenant_id: 필수.
        file_name: 필수. ``knowledge_files.file_name`` 매칭 — 내부에서 source_ref 해석.
        batch_size: batch 당 페이지 수. 5~50, 기본 20.
        parallelism: 동시 LLM 호출 수. 1~16, 기본 5.
        force_refresh: True 면 캐시 무시하고 강제 재생성. 기본 False.

    Returns:
        ``{file_name, n_pages, n_batches, batch_size, abstract, batches, errors, cached}``
        batches: ``[{batch_range, page_start, page_end, page_count, summary, ok, error?}, ...]``
        cached: 캐시에서 반환됐는지 여부.
    """
    if not tenant_id or not file_name:
        raise HTTPException(status_code=400, detail="tenant_id, file_name required")

    file_id = await _resolve_file_id(tenant_id, file_name)
    if not file_id:
        raise HTTPException(
            status_code=404,
            detail=f"file_name '{file_name}' not found in tenant '{tenant_id}'",
        )

    try:
        result = await svc_summarize_document(
            tenant_id=tenant_id,
            file_id=file_id,
            batch_size=batch_size,
            parallelism=parallelism,
            force_refresh=force_refresh,
        )
    except Exception as e:
        logger.exception("[/summarize] failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    logger.info(
        "[/summarize] tenant=%s file=%s n_pages=%d n_batches=%d batch_size=%d "
        "ok=%d errors=%d cached=%s",
        tenant_id, file_name, result.get("n_pages", 0), result.get("n_batches", 0),
        result.get("batch_size", batch_size),
        sum(1 for b in result.get("batches", []) if b.get("ok")),
        len(result.get("errors", [])),
        result.get("cached", False),
    )

    # file_name 보정 — service 가 knowledge_files 에서 가져온 값과 사용자 입력이 같아야
    if not result.get("file_name"):
        result["file_name"] = file_name

    return result
