"""document_pages 서비스 — 페이지 단위 저장 + 문서 카드 생성.

지도(catalog + grep + page-read)의 인제스트 측. ``load_document()`` 직후 페이지 단위
Document 들을 ``document_pages`` 에 INSERT 하고, 문서 카드 생성을 백그라운드로 넘긴다.
페이지가 저장되면 에이전트가 그 문서를 읽을 수 있으므로 저장된 페이지 수가 성공 기준이다.

실패는 격리(예외 안 던짐) — ingest 본 파이프라인이 계속 진행되도록.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain.schema import Document

from app.core.supabase_client import supabase

logger = logging.getLogger(__name__)


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


def _resolve_generation_model() -> str:
    try:
        from app.core.config import resolve_llm_config
        cfg = resolve_llm_config()
        return str(cfg.get("model") or "")
    except Exception:
        return ""


def _document_text(page_docs: List[Document]) -> str:
    """페이지 마커를 붙인 전문 — 카드 입력이자 codex 미러가 쓰는 형식과 같다."""
    blocks: List[str] = []
    for idx, doc in enumerate(page_docs):
        content = _normalize_page_text(doc.page_content or "").strip()
        if not content:
            continue
        blocks.append(f"=== page {_extract_page_number(doc, idx)} ===\n{content}")
    return "\n\n".join(blocks)


async def _resolve_file_name(tenant_id: str, file_id: str, fallback: str) -> str:
    """카드 제목에 스토리지 경로가 박히지 않게 실제 파일명을 찾는다."""
    if fallback and fallback != file_id and "/" not in fallback:
        return fallback
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("file_name")
            .eq("tenant_id", tenant_id)
            .eq("source_ref", file_id)
            .limit(1)
            .execute
        )
        rows = getattr(resp, "data", None) or []
        if rows and rows[0].get("file_name"):
            return str(rows[0]["file_name"])
    except Exception as e:
        logger.info("[document_pages] file_name 조회 실패 (%s): %s", file_id, e)
    return Path(fallback or file_id).name


async def build_and_store_card(
    tenant_id: str,
    file_id: str,
    file_name: str,
    page_docs: List[Document],
) -> None:
    """문서 전체를 슬라이딩 윈도우로 읽어 카드를 만들고 저장한다."""
    from app.services import doc_cards

    file_name = await _resolve_file_name(tenant_id, file_id, file_name)
    text = _document_text(page_docs)
    n_pages = sum(1 for doc in page_docs if _normalize_page_text(doc.page_content or ""))
    await doc_cards.save_text_stats(
        tenant_id=tenant_id, file_id=file_id, text=text, page_count=n_pages
    )
    if not text.strip():
        # 텍스트 레이어가 없는 문서. 카드가 아니라 '읽을 수 없음'이 사실이다.
        await doc_cards.save_card(
            tenant_id=tenant_id, file_id=file_id,
            card={"card_version": doc_cards.CARD_VERSION, "title": file_name, "summary": "",
                  "coverage": doc_cards.Coverage().as_dict()},
            signature="", content_hash="", status="empty",
        )
        return

    content_hash = doc_cards.content_sha256(text)
    reused = await doc_cards.load_existing_card(tenant_id, content_hash)
    if reused:
        logger.info("[document_pages] 같은 내용의 카드 재사용 file_id=%s", file_id)
        await doc_cards.save_card(
            tenant_id=tenant_id, file_id=file_id, card=reused,
            signature=doc_cards.card_signature(text=text, model=_resolve_generation_model()),
            content_hash=content_hash,
        )
        return

    context = await doc_cards.load_neighbors(tenant_id, file_id)
    async with doc_cards.card_gate():
        card = await doc_cards.build_card(file_name=file_name, text=text, context=context)
    # 모든 조각이 실패했으면 카드가 아니라 실패다. done 으로 묻으면 재시도 대상에서 빠진다.
    every_window_failed = (
        card.coverage.windows_read > 0
        and card.coverage.windows_failed >= card.coverage.windows_read
    )
    await doc_cards.save_card(
        tenant_id=tenant_id, file_id=file_id, card=card.as_dict(),
        signature=doc_cards.card_signature(text=text, model=_resolve_generation_model()),
        content_hash=content_hash,
        status="failed" if every_window_failed else "done",
    )


async def schedule_card_build(
    tenant_id: str,
    file_id: str,
    page_docs: List[Document],
    file_name: str = "",
) -> None:
    """카드 생성을 백그라운드로 넘긴다 — 업로드 응답이 카드를 기다리지 않는다."""
    name = file_name or str((page_docs[0].metadata or {}).get("source_path") or file_id)

    async def _run() -> None:
        from app.services import doc_cards

        # 진행 상태를 먼저 남긴다 — 조회하는 쪽이 '아직 없음'과 '만드는 중'을 구분한다.
        await doc_cards.save_card(
            tenant_id=tenant_id, file_id=file_id, card={}, signature="",
            content_hash="", status="pending",
        )
        try:
            await build_and_store_card(tenant_id, file_id, name, page_docs)
        except Exception as exc:  # noqa: BLE001 - 카드 실패가 인제스트를 오염시키지 않는다
            logger.warning("[document_pages] 카드 생성 실패 (%s/%s): %s", tenant_id, file_id, exc)
            await doc_cards.save_card(
                tenant_id=tenant_id, file_id=file_id, card={"error": str(exc)[:300]},
                signature="", content_hash="", status="failed",
            )

    asyncio.create_task(_run())


async def post_load_hook(
    tenant_id: Optional[str],
    file_id: Optional[str],
    page_docs: List[Document],
    *,
    skip_abstract: bool = False,
) -> int:
    """``load_document()`` 직후 호출 — 페이지 저장 + 문서 카드 생성.

    페이지가 저장되면 에이전트가 그 문서를 읽을 수 있다. 그래서 이 반환값(저장된 페이지 수)이
    인제스트 성공의 기준이다. 카드는 백그라운드로 돌고, 실패해도 페이지는 남는다.

    Args:
        skip_abstract: True 면 카드 생성을 생략 (페이지 저장만). 용어사전·양식 등
            카드가 의미 없는 doc_role 에서 사용.
    """
    if not tenant_id or not file_id or not page_docs:
        return 0

    try:
        saved = await save_pages(tenant_id, file_id, page_docs)
        if saved and not skip_abstract:
            await schedule_card_build(tenant_id, file_id, page_docs)
        return saved
    except Exception as e:
        logger.warning(
            "[document_pages] post_load_hook failed (%s/%s): %s",
            tenant_id, file_id, e,
        )
        return 0
