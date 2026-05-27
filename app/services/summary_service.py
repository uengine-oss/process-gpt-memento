"""문서 요약 서비스 — page batch 단위 병렬 LLM 처리.

흐름:
  1. document_pages 에서 해당 file 의 모든 페이지 조회 (page_number 정렬)
  2. batch_size 단위로 분할
  3. 각 batch 에 대해 mini-summary 프롬프트 빌드 → asyncio.gather 로 병렬 LLM 콜
     (Semaphore 로 동시도 제한해 vLLM/외부 API 부담 컷)
  4. batch 별 결과를 dict 리스트로 반환

agent 무관 — 순수 코드 처리. deep-agents-temp 의 summarize_document 도구가
이걸 HTTP 로 호출. 통합/최종 요약은 main 에이전트가 작성하므로 *여기는 mini-summary 까지만*.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.supabase_client import supabase
from app.services.llm import create_llm
from app.services.summary_prompts import build_mini_summary_prompt


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 캐시 (knowledge_files.doc_summary jsonb 컬럼)
# ─────────────────────────────────────────────────────────────────────────────
# 매칭 키: (tenant_id, file_id). 같은 file_id 면 cache hit.
# batch_size 가 다르면 캐시된 결과를 사용해도 의미가 달라지므로 hit 시 batch_size 도 확인.
# 파일 재인덱싱 시 document_pages._delete_existing_pages 와 함께 NULL 로 무효화된다.


async def _load_cached_summary(
    tenant_id: str, file_id: str, batch_size: int
) -> Optional[Dict[str, Any]]:
    """knowledge_files.doc_summary 에서 cache 조회. batch_size 다르면 miss."""
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("doc_summary")
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .limit(1)
            .execute
        )
        rows = resp.data or []
        if not rows:
            return None
        cached = rows[0].get("doc_summary")
        if not isinstance(cached, dict):
            return None
        # batch_size 가 다르면 분할 단위가 달라 의미상 stale — miss.
        if int(cached.get("batch_size") or 0) != int(batch_size):
            return None
        return {
            "n_pages": int(cached.get("n_pages") or 0),
            "n_batches": int(cached.get("n_batches") or 0),
            "batch_size": int(cached.get("batch_size") or batch_size),
            "abstract": cached.get("abstract") or "",
            "batches": cached.get("batches") or [],
            "generated_at": cached.get("generated_at"),
            "model": cached.get("model"),
        }
    except Exception as e:
        logger.warning(
            "[summary_service] cache lookup failed (%s/%s): %s",
            tenant_id, file_id, e,
        )
        return None


async def _save_summary_cache(
    tenant_id: str,
    file_id: str,
    batch_size: int,
    n_pages: int,
    n_batches: int,
    abstract: str,
    batches: List[Dict[str, Any]],
    model: str,
) -> None:
    """knowledge_files.doc_summary 컬럼 UPDATE. 실패해도 응답에는 영향 없음 (로깅만)."""
    payload = {
        "batch_size": batch_size,
        "n_pages": n_pages,
        "n_batches": n_batches,
        "abstract": abstract,
        "batches": batches,
        "model": model,
        # ISO 문자열 — jsonb 안이라 timestamptz 변환 안 됨.
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update({"doc_summary": payload})
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .execute
        )
    except Exception as e:
        logger.warning(
            "[summary_service] cache save failed (%s/%s): %s",
            tenant_id, file_id, e,
        )


async def invalidate_summary_cache(tenant_id: str, file_id: str) -> None:
    """파일 재인덱싱 등으로 본문이 바뀌었을 때 doc_summary 를 NULL 로 무효화."""
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update({"doc_summary": None})
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .execute
        )
        logger.info(
            "[summary_service] cache invalidated tenant=%s file_id=%s",
            tenant_id, file_id,
        )
    except Exception as e:
        logger.warning(
            "[summary_service] cache invalidate failed (%s/%s): %s",
            tenant_id, file_id, e,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 가드 / 기본값
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_BATCH_SIZE = 20         # batch 당 페이지 수
MAX_BATCH_SIZE = 50             # 안전 cap
MIN_BATCH_SIZE = 5
MAX_PARALLEL_LLM_CALLS = 5      # 동시 LLM 호출 수
PER_PAGE_CHAR_LIMIT = 8000      # 페이지가 너무 길면 잘라서 (tail 제거)

# 한 batch 당 LLM 호출 timeout (초)
LLM_TIMEOUT = (10.0, 120.0)
LLM_MAX_RETRIES = 2


# ─────────────────────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────────────────────

async def _load_pages(tenant_id: str, file_id: str) -> List[Dict[str, Any]]:
    """document_pages 에서 page_number 정렬로 모든 페이지 로드."""
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
            "[summary_service] load pages failed (tenant=%s, file=%s): %s",
            tenant_id, file_id, e,
        )
        return []


async def _resolve_doc_meta(tenant_id: str, file_id: str) -> Dict[str, Any]:
    """knowledge_files 에서 file_name + doc_card abstract 조회."""
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
        return {
            "file_name": row.get("file_name") or "",
            "abstract": (card.get("abstract") or "") if isinstance(card, dict) else "",
        }
    except Exception as e:
        logger.warning(
            "[summary_service] resolve meta failed (tenant=%s, file=%s): %s",
            tenant_id, file_id, e,
        )
        return {"file_name": "", "abstract": ""}


def _split_into_batches(
    pages: List[Dict[str, Any]], batch_size: int
) -> List[List[Dict[str, Any]]]:
    """pages 를 batch_size 단위로 분할. 마지막 batch 는 짧을 수 있음."""
    if batch_size < 1:
        batch_size = DEFAULT_BATCH_SIZE
    return [pages[i:i + batch_size] for i in range(0, len(pages), batch_size)]


def _trim_page_text(text: str, limit: int = PER_PAGE_CHAR_LIMIT) -> str:
    """페이지 본문 한도 컷 — 표·이미지 캡션이 많아 한 페이지 수십 KB 되는 경우 보호."""
    if not isinstance(text, str):
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "\n[...해당 페이지 본문 길이 초과로 일부 생략...]"


def _build_batch_text(batch_pages: List[Dict[str, Any]]) -> str:
    """batch 내 페이지들을 ## p.N 헤더 + 본문으로 합침."""
    parts: List[str] = []
    for p in batch_pages:
        page_num = p.get("page_number")
        body = _trim_page_text(p.get("content") or "")
        if not body.strip():
            continue
        parts.append(f"## p.{page_num}\n\n{body}")
    return "\n\n".join(parts)


def _clean_llm_output(text: str) -> str:
    """모델이 흔히 붙이는 코드펜스·머리말 정리."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    for prefix in ("[정리]", "정리:", "요약:", "Summary:", "summary:"):
        if text.startswith(prefix):
            text = text[len(prefix):].lstrip()
            break
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

async def summarize_document(
    tenant_id: str,
    file_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    parallelism: int = MAX_PARALLEL_LLM_CALLS,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """문서 1개를 page batch 단위로 병렬 mini-summary.

    Args:
        tenant_id: 필수
        file_id: knowledge_files.source_ref / document_pages.file_id
        batch_size: batch 당 페이지 수 (5~50)
        parallelism: 동시 LLM 호출 한도

    Returns:
      {
        "file_name": "...",
        "n_pages": int,
        "n_batches": int,
        "batch_size": int,
        "abstract": "...",         # doc_card abstract (있으면)
        "batches": [
          {"batch_range": "1-20", "page_start": 1, "page_end": 20,
           "page_count": 20, "summary": "- ...\n- ...", "ok": True},
          ...
        ],
        "errors": [...]
      }
    """
    if not tenant_id or not file_id:
        return {
            "file_name": "", "n_pages": 0, "n_batches": 0,
            "batch_size": batch_size, "abstract": "", "batches": [],
            "errors": ["tenant_id and file_id required"],
        }

    # 가드 — batch_size 범위
    bs = max(MIN_BATCH_SIZE, min(int(batch_size or DEFAULT_BATCH_SIZE), MAX_BATCH_SIZE))
    par = max(1, min(int(parallelism or MAX_PARALLEL_LLM_CALLS), 16))

    # 캐시 hit 조회 — knowledge_files.doc_summary
    if not force_refresh:
        cached = await _load_cached_summary(tenant_id, file_id, bs)
        if cached:
            logger.info(
                "[summary_service] cache HIT tenant=%s file=%s batch_size=%d n_batches=%d",
                tenant_id, file_id, bs, cached.get("n_batches", 0),
            )
            # file_name 은 응답 형식 유지를 위해 meta 에서 한 번 더 조회 (캐시엔 안 박음).
            meta = await _resolve_doc_meta(tenant_id, file_id)
            return {
                "file_name": meta.get("file_name", ""),
                "n_pages": cached["n_pages"],
                "n_batches": cached["n_batches"],
                "batch_size": cached["batch_size"],
                "abstract": cached["abstract"] or meta.get("abstract", ""),
                "batches": cached["batches"],
                "errors": [],
                "cached": True,
            }

    # 메타 + 페이지 동시 로드
    meta_task = asyncio.create_task(_resolve_doc_meta(tenant_id, file_id))
    pages_task = asyncio.create_task(_load_pages(tenant_id, file_id))
    meta = await meta_task
    pages = await pages_task

    n_pages = len(pages)
    if n_pages == 0:
        return {
            "file_name": meta.get("file_name", ""), "n_pages": 0, "n_batches": 0,
            "batch_size": bs, "abstract": meta.get("abstract", ""), "batches": [],
            "errors": ["no pages found in document_pages — file not ingested or empty"],
        }

    batches = _split_into_batches(pages, bs)
    n_batches = len(batches)
    logger.info(
        "[summary_service] start tenant=%s file=%s n_pages=%d n_batches=%d batch_size=%d par=%d",
        tenant_id, file_id, n_pages, n_batches, bs, par,
    )

    # LLM (모듈 단위 단일 인스턴스 재사용 가능하나, create_llm 가벼움)
    llm = create_llm(temperature=0.0, timeout=LLM_TIMEOUT, max_retries=LLM_MAX_RETRIES)

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
            return {
                "batch_range": batch_range, "page_start": page_start, "page_end": page_end,
                "page_count": page_count, "summary": "", "ok": False,
                "error": "empty batch text",
            }

        prompt = build_mini_summary_prompt(
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
                summary = _clean_llm_output(raw)
                logger.info(
                    "[summary_service] batch %d/%d (p.%s) done — %d chars",
                    idx + 1, n_batches, batch_range, len(summary),
                )
                return {
                    "batch_range": batch_range, "page_start": page_start, "page_end": page_end,
                    "page_count": page_count, "summary": summary, "ok": True,
                }
            except Exception as e:
                logger.warning(
                    "[summary_service] batch %d/%d (p.%s) failed: %s",
                    idx + 1, n_batches, batch_range, e,
                )
                return {
                    "batch_range": batch_range, "page_start": page_start, "page_end": page_end,
                    "page_count": page_count, "summary": "", "ok": False,
                    "error": str(e)[:200],
                }

    results = await asyncio.gather(*[
        process_one(i, b) for i, b in enumerate(batches)
    ])

    # 실패 batch 들 모음 (errors 필드)
    for r in results:
        if not r.get("ok") and r.get("error"):
            errors.append(f"batch p.{r['batch_range']}: {r['error']}")

    ok_count = sum(1 for r in results if r.get("ok"))
    logger.info(
        "[summary_service] done tenant=%s file=%s ok=%d/%d",
        tenant_id, file_id, ok_count, n_batches,
    )

    # 캐시 저장 — 전체 batch ok 일 때만. 부분 실패면 다음 호출에서 재시도 기회.
    if ok_count == n_batches and n_batches > 0:
        model_name = getattr(llm, "model_name", None) or getattr(llm, "model", "") or ""
        await _save_summary_cache(
            tenant_id=tenant_id,
            file_id=file_id,
            batch_size=bs,
            n_pages=n_pages,
            n_batches=n_batches,
            abstract=meta.get("abstract", ""),
            batches=results,
            model=str(model_name),
        )

    return {
        "file_name": meta.get("file_name", ""),
        "n_pages": n_pages,
        "n_batches": n_batches,
        "batch_size": bs,
        "abstract": meta.get("abstract", ""),
        "batches": results,
        "errors": errors,
        "cached": False,
    }
