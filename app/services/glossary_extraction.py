"""용어 사전 정제 추출 — page batch 단위 병렬 LLM 처리.

doc_role='glossary' 자료 ingest 시 호출.
batch 별로 LLM 이 *형식 자유* 정제 텍스트(한 줄 = 한 용어)를 뽑으면,
이 모듈이 batch 결과들을 ``### p.X-Y`` 헤더 붙여 단순 concat 한다 (merge/dedup 없음).

저장 위치: knowledge_files.glossary_compact (전용 TEXT 컬럼)

흐름:
    upload(doc_role='glossary') → save_pages → ★ extract_and_save_glossary_compact()

채팅 진입 시 /glossary/inline 이 glossary_compact 우선 반환 (raw page fallback).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

from app.core.supabase_client import supabase
from app.services.glossary_prompts import build_glossary_extract_prompt
from app.services.llm import create_llm


logger = logging.getLogger(__name__)


# 가드 / 기본값 — summary_service 와 동일 기조
DEFAULT_BATCH_SIZE = 20
MAX_BATCH_SIZE = 50
MIN_BATCH_SIZE = 5
MAX_PARALLEL_LLM_CALLS = 5
PER_PAGE_CHAR_LIMIT = 8000

LLM_TIMEOUT = (10.0, 120.0)
LLM_MAX_RETRIES = 2

# 추출 결과가 비정상적으로 비대해지는 케이스 보호 (raw 와 차이 거의 없음)
MAX_COMPACT_CHARS = 200_000


# ─────────────────────────────────────────────────────────────────────────────
# 페이지 / 메타 로딩
# ─────────────────────────────────────────────────────────────────────────────

async def _load_pages(tenant_id: str, file_id: str) -> List[Dict[str, Any]]:
    try:
        resp = await asyncio.to_thread(
            supabase.table("document_pages")
            .select("page_number, content")
            .eq("tenant_id", tenant_id)
            .eq("file_id", file_id)
            .order("page_number", desc=False)
            .execute
        )
        return resp.data or []
    except Exception as e:
        logger.warning(
            "[glossary_extraction] load pages failed (tenant=%s, file=%s): %s",
            tenant_id, file_id, e,
        )
        return []


async def _resolve_doc_meta(tenant_id: str, file_id: str) -> Dict[str, Any]:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("file_name, doc_card")
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .limit(1)
            .execute
        )
        rows = resp.data or []
        if not rows:
            return {"file_name": "", "abstract": ""}
        row = rows[0]
        card = row.get("doc_card") or {}
        if not isinstance(card, dict):
            card = {}
        return {
            "file_name": row.get("file_name") or "",
            "abstract": card.get("abstract") or "",
        }
    except Exception as e:
        logger.warning("[glossary_extraction] resolve meta failed: %s", e)
        return {"file_name": "", "abstract": ""}


def _split_into_batches(
    pages: List[Dict[str, Any]], batch_size: int
) -> List[List[Dict[str, Any]]]:
    if batch_size < 1:
        batch_size = DEFAULT_BATCH_SIZE
    return [pages[i:i + batch_size] for i in range(0, len(pages), batch_size)]


def _trim_page_text(text: str, limit: int = PER_PAGE_CHAR_LIMIT) -> str:
    if not isinstance(text, str):
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "\n[...해당 페이지 본문 길이 초과로 일부 생략...]"


def _build_batch_text(batch_pages: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for p in batch_pages:
        page_num = p.get("page_number")
        body = _trim_page_text(p.get("content") or "")
        if not body.strip():
            continue
        parts.append(f"## p.{page_num}\n\n{body}")
    return "\n\n".join(parts)


def _clean_llm_output(text: str) -> str:
    """LLM 출력 정리 — 코드펜스/앞뒤 공백 제거. 내부 형식은 건드리지 않음."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    if text.startswith("```"):
        nl = text.find("\n")
        if nl != -1:
            text = text[nl + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    return text


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

async def extract_glossary_compact(
    tenant_id: str,
    file_id: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    parallelism: int = MAX_PARALLEL_LLM_CALLS,
) -> Dict[str, Any]:
    """문서 1개의 용어 매핑을 batch 병렬 추출 + concat.

    Returns:
        {
          "ok": bool,                    # 1개 이상 batch 성공이면 True
          "compact": str,                # 합쳐진 정제본 (마크다운, batch 헤더 포함)
          "n_pages": int,
          "n_batches": int,
          "batch_size": int,
          "ok_batches": int,
          "model": str,
          "errors": list[str],
        }
    """
    if not tenant_id or not file_id:
        return {
            "ok": False, "compact": "", "n_pages": 0, "n_batches": 0,
            "batch_size": batch_size, "ok_batches": 0, "model": "",
            "errors": ["tenant_id and file_id required"],
        }

    bs = max(MIN_BATCH_SIZE, min(int(batch_size or DEFAULT_BATCH_SIZE), MAX_BATCH_SIZE))
    par = max(1, min(int(parallelism or MAX_PARALLEL_LLM_CALLS), 16))

    meta_task = asyncio.create_task(_resolve_doc_meta(tenant_id, file_id))
    pages_task = asyncio.create_task(_load_pages(tenant_id, file_id))
    meta = await meta_task
    pages = await pages_task

    n_pages = len(pages)
    if n_pages == 0:
        return {
            "ok": False, "compact": "", "n_pages": 0, "n_batches": 0,
            "batch_size": bs, "ok_batches": 0, "model": "",
            "errors": ["no pages — file not ingested or empty"],
        }

    batches = _split_into_batches(pages, bs)
    n_batches = len(batches)
    file_name = meta.get("file_name") or file_id
    logger.info(
        "[glossary_extraction] start tenant=%s file=%s n_pages=%d n_batches=%d bs=%d par=%d",
        tenant_id, file_name, n_pages, n_batches, bs, par,
    )

    llm = create_llm(temperature=0.0, timeout=LLM_TIMEOUT, max_retries=LLM_MAX_RETRIES)
    model_name = getattr(llm, "model_name", None) or getattr(llm, "model", None) or ""

    sem = asyncio.Semaphore(par)
    errors: List[str] = []

    async def process_one(idx: int, batch_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
        page_nums = [p.get("page_number") for p in batch_pages if p.get("page_number")]
        page_start = min(page_nums) if page_nums else 0
        page_end = max(page_nums) if page_nums else 0
        page_count = len(batch_pages)
        batch_range = f"{page_start}-{page_end}" if page_count > 1 else f"{page_start}"

        batch_text = _build_batch_text(batch_pages)
        if not batch_text.strip():
            return {"ok": False, "text": "", "batch_range": batch_range, "error": "empty batch"}

        prompt = build_glossary_extract_prompt(
            doc_abstract=meta.get("abstract", ""),
            n_pages=n_pages,
            batch_start=page_start,
            batch_end=page_end,
            page_count=page_count,
            batch_text=batch_text,
        )

        async with sem:
            try:
                response = await llm.ainvoke(prompt)
                raw = getattr(response, "content", response)
                if not isinstance(raw, str):
                    raw = str(raw)
                cleaned = _clean_llm_output(raw)
                logger.info(
                    "[glossary_extraction] batch %d/%d (p.%s) → %d chars",
                    idx + 1, n_batches, batch_range, len(cleaned),
                )
                return {"ok": True, "text": cleaned, "batch_range": batch_range}
            except Exception as e:
                logger.warning(
                    "[glossary_extraction] batch %d/%d (p.%s) failed: %s",
                    idx + 1, n_batches, batch_range, e,
                )
                return {
                    "ok": False, "text": "", "batch_range": batch_range,
                    "error": str(e)[:200],
                }

    results = await asyncio.gather(*[
        process_one(i, b) for i, b in enumerate(batches)
    ])

    # batch 결과 단순 concat — ### p.X-Y 헤더 + 본문
    parts: List[str] = [f"## {file_name} — 정제본"]
    ok_batches = 0
    for r in results:
        br = r.get("batch_range", "")
        if r.get("ok") and r.get("text"):
            parts.append(f"\n### p.{br}\n{r['text']}")
            ok_batches += 1
        else:
            err = r.get("error") or "unknown error"
            errors.append(f"batch p.{br}: {err}")

    compact = "\n".join(parts) + "\n"

    # 비대 케이스 컷
    if len(compact) > MAX_COMPACT_CHARS:
        logger.warning(
            "[glossary_extraction] compact too large (%d chars), truncating to %d",
            len(compact), MAX_COMPACT_CHARS,
        )
        compact = compact[:MAX_COMPACT_CHARS] + "\n[...정제본 길이 초과로 일부 생략...]"

    logger.info(
        "[glossary_extraction] done tenant=%s file=%s ok=%d/%d compact_chars=%d",
        tenant_id, file_name, ok_batches, n_batches, len(compact),
    )
    return {
        "ok": ok_batches > 0,
        "compact": compact,
        "n_pages": n_pages,
        "n_batches": n_batches,
        "batch_size": bs,
        "ok_batches": ok_batches,
        "model": model_name,
        "errors": errors,
    }


async def update_glossary_compact(
    tenant_id: str,
    file_id: str,
    compact: str,
) -> bool:
    """추출 결과를 knowledge_files.glossary_compact 컬럼에 저장.

    빈 문자열은 저장 안 함 — 호출자가 ok 체크 후 호출.
    """
    if not tenant_id or not file_id or not compact:
        return False
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update({"glossary_compact": compact})
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .execute
        )
        return True
    except Exception as e:
        logger.warning(
            "[glossary_extraction] update glossary_compact failed (%s/%s): %s",
            tenant_id, file_id, e,
        )
        return False


async def extract_and_save_glossary_compact(
    tenant_id: str,
    file_id: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    parallelism: int = MAX_PARALLEL_LLM_CALLS,
) -> Dict[str, Any]:
    """upload endpoint 에서 호출하는 one-shot helper.

    extract → save 까지 묶고 결과 dict 반환. 실패는 격리 — caller 가 mark_status 결정.
    """
    result = await extract_glossary_compact(
        tenant_id, file_id, batch_size=batch_size, parallelism=parallelism
    )
    saved = False
    if result.get("ok"):
        saved = await update_glossary_compact(tenant_id, file_id, result["compact"])
    result["saved"] = saved
    return result
