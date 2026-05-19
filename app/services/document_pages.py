"""document_pages 서비스 — 페이지 단위 저장 + doc_card 생성.

agent navigation(catalog + grep + page-read) 인프라의 ingest 측.
기존 청크 RAG 파이프라인은 그대로 두고, 페이지 데이터를 *추가로* 저장한다.

호출 시점: ``load_document()`` 직후 / ``process_documents()`` 호출 직전.
페이지 단위 Document 들을 받아 ``document_pages`` 테이블에 INSERT 하고,
별도로 abstract LLM 콜을 던져 ``knowledge_files.doc_card`` 를 채운다.

실패는 격리(예외 안 던짐) — ingest 본 파이프라인이 계속 진행되도록.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from langchain.schema import Document

from app.core.supabase_client import supabase
from app.services.llm import create_llm

logger = logging.getLogger(__name__)


# abstract 생성 시 LLM 에 보여줄 페이지 범위 — 앞 N + 뒤 M
_FIRST_N_PAGES = 3
_LAST_N_PAGES = 1
# 페이지 본문이 너무 길면 abstract 프롬프트에선 잘라서. 본 저장은 전체 유지.
_PAGE_TEXT_LIMIT_FOR_ABSTRACT = 4000


def _normalize_page_text(text: str) -> str:
    """페이지 본문에서 NUL 등 Postgres 거부 문자 제거 + 양끝 공백 정리."""
    if not isinstance(text, str):
        return ""
    return text.replace("\x00", "").strip()


def _extract_page_number(doc: Document, fallback_index: int) -> int:
    """Document 메타에서 1-based 페이지 번호 추출.

    PDF 파서는 ``metadata['page']`` 에 0-based 페이지를 넣는다.
    페이지 정보가 없는 단일 컨텐츠 문서(.docx, .txt 등)는 ``fallback_index + 1``.
    """
    meta = doc.metadata or {}
    if meta.get("page_number") is not None:
        try:
            return int(meta["page_number"])
        except (TypeError, ValueError):
            pass
    if meta.get("page") is not None:
        try:
            return int(meta["page"]) + 1
        except (TypeError, ValueError):
            pass
    return fallback_index + 1


async def _delete_existing_pages(tenant_id: str, file_id: str) -> None:
    """재인덱싱 시 기존 페이지 row 삭제 — UNIQUE 충돌 방지.

    같은 시점에 summary-pipeline 캐시(knowledge_files.doc_summary)도 무효화한다.
    본문이 바뀌었는데 옛 요약을 반환하면 안 되기 때문.
    """
    try:
        await asyncio.to_thread(
            supabase.table("document_pages")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("file_id", file_id)
            .execute
        )
    except Exception as e:
        logger.warning(
            "[document_pages] delete existing failed (%s/%s): %s",
            tenant_id, file_id, e,
        )
    # 캐시 무효화 — import 사이클 회피를 위해 함수 내에서 lazy import.
    try:
        from app.services.summary_service import invalidate_summary_cache
        await invalidate_summary_cache(tenant_id, file_id)
    except Exception as e:
        logger.warning(
            "[document_pages] summary cache invalidate failed (%s/%s): %s",
            tenant_id, file_id, e,
        )


async def save_pages(
    tenant_id: str,
    file_id: str,
    page_docs: List[Document],
) -> int:
    """페이지 단위 Document 들을 ``document_pages`` 테이블에 INSERT.

    Args:
        tenant_id: 테넌트
        file_id: ``knowledge_files.source_ref`` 와 동일 값 (drive: 파일ID / upload: storage path)
        page_docs: ``load_document()`` 이 반환한 페이지 단위 Document 리스트

    Returns:
        INSERT 한 row 수.
    """
    if not tenant_id or not file_id or not page_docs:
        return 0

    await _delete_existing_pages(tenant_id, file_id)

    rows: List[Dict[str, Any]] = []
    for idx, doc in enumerate(page_docs):
        content = _normalize_page_text(doc.page_content or "")
        if not content:
            continue
        page_number = _extract_page_number(doc, idx)
        meta = doc.metadata or {}
        # page_meta 는 작게만 — 노이즈 큰 필드(이미지 추출물 등) 제외
        safe_meta: Dict[str, Any] = {}
        for key in ("page_width", "page_height", "source_path"):
            val = meta.get(key)
            if isinstance(val, (str, int, float, bool)):
                safe_meta[key] = val
        rows.append({
            "tenant_id": tenant_id,
            "file_id": file_id,
            "page_number": page_number,
            "content": content,
            "page_meta": safe_meta,
        })

    if not rows:
        return 0

    try:
        batch_size = 200
        inserted = 0
        for start in range(0, len(rows), batch_size):
            chunk = rows[start:start + batch_size]
            await asyncio.to_thread(
                supabase.table("document_pages").insert(chunk).execute
            )
            inserted += len(chunk)
        logger.info(
            "[document_pages] saved tenant=%s file_id=%s pages=%d",
            tenant_id, file_id, inserted,
        )
        return inserted
    except Exception as e:
        logger.warning(
            "[document_pages] insert failed (%s/%s): %s",
            tenant_id, file_id, e,
        )
        return 0


def _build_abstract_prompt(page_docs: List[Document]) -> str:
    """앞 N + 뒤 M 페이지 텍스트만 모아 프롬프트 구성."""
    total = len(page_docs)
    if total == 0:
        return ""

    if total <= _FIRST_N_PAGES + _LAST_N_PAGES:
        selected = list(enumerate(page_docs))
    else:
        head = list(enumerate(page_docs[:_FIRST_N_PAGES]))
        tail_start = total - _LAST_N_PAGES
        tail = [(tail_start + i, page_docs[tail_start + i]) for i in range(_LAST_N_PAGES)]
        selected = head + tail

    parts: List[str] = []
    for idx, doc in selected:
        page_num = _extract_page_number(doc, idx)
        text = _normalize_page_text(doc.page_content or "")[:_PAGE_TEXT_LIMIT_FOR_ABSTRACT]
        if not text:
            continue
        parts.append(f"--- p.{page_num} ---\n{text}")

    body = "\n\n".join(parts)
    if not body:
        return ""

    return (
        "다음은 어떤 문서의 앞부분과 마지막 페이지다. "
        "이 문서가 무엇인지 1~2 문장의 한국어 평문으로 적어라.\n"
        "여기에 없는 사실을 추가하지 마라. 추측 금지.\n"
        "코드펜스·JSON·따옴표·머리말 없이 *답변 문장만* 출력하라.\n\n"
        f"{body}\n\n"
        "이 문서의 요약:"
    )


def _clean_abstract_output(text: str) -> str:
    """모델이 흔히 붙이는 코드펜스·따옴표·머리말을 정리."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    # ```json ... ``` 형식 제거
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[: -3]
        text = text.strip()
    # 양 끝 따옴표
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        text = text[1:-1].strip()
    # 머리말 패턴 ("요약:", "Abstract:", ...) 제거
    for prefix in ("요약:", "Abstract:", "abstract:", "Summary:"):
        if text.startswith(prefix):
            text = text[len(prefix):].lstrip()
            break
    return text.strip()


async def _generate_abstract(page_docs: List[Document]) -> Optional[str]:
    """abstract 1회 LLM 콜.

    실패 시 ``None`` 반환 — caller 가 fallback(null) 처리.
    """
    prompt = _build_abstract_prompt(page_docs)
    if not prompt:
        return None
    try:
        llm = create_llm(temperature=0.0, timeout=(10.0, 60.0), max_retries=2)
        response = await llm.ainvoke(prompt)
        raw = getattr(response, "content", response)
        if not isinstance(raw, str):
            raw = str(raw)
        cleaned = _clean_abstract_output(raw)
        return cleaned or None
    except Exception as e:
        logger.warning("[document_pages] abstract LLM failed: %s", e)
        return None


def _resolve_generation_model() -> str:
    try:
        from app.core.config import resolve_llm_config
        cfg = resolve_llm_config()
        return str(cfg.get("model") or "")
    except Exception:
        return ""


async def update_doc_card(
    tenant_id: str,
    file_id: str,
    page_docs: List[Document],
) -> bool:
    """abstract 생성 후 ``knowledge_files.doc_card`` UPDATE.

    매칭 키: ``(tenant_id, source_ref=file_id)``. source_type 는 drive/upload 어느 쪽이든
    source_ref 가 unique 하므로 조건에서 제외.
    """
    if not tenant_id or not file_id:
        return False

    abstract = await _generate_abstract(page_docs)
    n_pages = sum(1 for d in page_docs if _normalize_page_text(d.page_content or ""))

    card: Dict[str, Any] = {
        "abstract": abstract,
        "n_pages": n_pages,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "generation_model": _resolve_generation_model(),
    }

    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update({"doc_card": card})
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .execute
        )
        logger.info(
            "[document_pages] doc_card updated tenant=%s file_id=%s abstract=%s n_pages=%d",
            tenant_id, file_id, "ok" if abstract else "null", n_pages,
        )
        return True
    except Exception as e:
        logger.warning(
            "[document_pages] doc_card update failed (%s/%s): %s",
            tenant_id, file_id, e,
        )
        return False


async def post_load_hook(
    tenant_id: Optional[str],
    file_id: Optional[str],
    page_docs: List[Document],
    *,
    skip_abstract: bool = False,
) -> None:
    """``load_document()`` 직후 호출 — 페이지 저장 + abstract 생성.

    ``tenant_id`` / ``file_id`` 둘 다 있어야 동작(로컬 dev 경로 등 file_id 없으면 noop).
    페이지 INSERT 와 abstract LLM 콜은 *병렬* — abstract 가 INSERT 를 막지 않도록.
    실패는 격리 — 본 ingest 파이프라인이 계속 가게.

    Args:
        skip_abstract: True 면 abstract LLM 콜을 생략 (페이지 저장만). 용어사전·양식 등
            abstract 가 의미 없는 doc_role 에서 사용. 기본 False — 기존 호출자는 영향 없음.
    """
    if not tenant_id or not file_id or not page_docs:
        return

    try:
        if skip_abstract:
            await save_pages(tenant_id, file_id, page_docs)
        else:
            await asyncio.gather(
                save_pages(tenant_id, file_id, page_docs),
                update_doc_card(tenant_id, file_id, page_docs),
                return_exceptions=True,
            )
    except Exception as e:
        logger.warning(
            "[document_pages] post_load_hook failed (%s/%s): %s",
            tenant_id, file_id, e,
        )
