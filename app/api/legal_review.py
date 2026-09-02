"""legal_review(검토 사례) 검색 라우터 — 계약 검토용 2단계 선례 검색.

GET /legal-review/precedents
  1단계: 검토 대상 계약의 *사업배경* 텍스트 → type=background 임베딩 유사도 → top-K 유사 선례 doc
  2단계: 그 doc 들의 type=clause 청크(조항 + 변호사 메모) 회수 → 조항 단위로 조립해 반환

인덱스(doc_role=legal_review)는 memento 가 소유하므로 2단계 조립을 *서버측* 에서 수행한다.
deepagents-lite 의 nda-reviewer 서브에이전트가 이 엔드포인트를 호출해 先사례를 가져온다.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.vector_store import get_vector_store

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/legal-review/precedents")
async def legal_review_precedents(
    tenant_id: str,
    background: str,
    top_k: int = Query(default=3, ge=1, le=10),
    contract_type: Optional[str] = None,
):
    """사업배경 유사 선례(legal_review) 의 조항+변호사메모를 2단계로 회수.

    Args:
        tenant_id: 테넌트.
        background: 검토 대상 계약의 사업배경 텍스트(임베딩 쿼리).
        top_k: 유사 선례 doc 수.
        contract_type: 'nda'/'mou' 등으로 선례 종류 한정(옵션).

    Returns:
        ``{"response": [{file_id, file_name, contract_type, profile,
                          background_summary, clauses:[{clause_no, clause_title,
                          topic, text, memos:[...]}]}, ...]}``
    """
    if not tenant_id or not (background or "").strip():
        raise HTTPException(status_code=400, detail="tenant_id, background required")

    vsm = get_vector_store()

    # ── 1단계: 사업배경 유사 선례 doc 검색 (type=background) ──
    bg_filter: Dict[str, Any] = {
        "tenant_id": tenant_id,
        "doc_role": "legal_review",
        "type": "background",
    }
    if contract_type:
        bg_filter["contract_type"] = contract_type

    try:
        bg_docs = await vsm.similarity_search(background, filter=bg_filter, top_k=top_k)
    except Exception as e:  # noqa: BLE001
        logger.exception("[/legal-review/precedents] stage1 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    precedents: List[Dict[str, Any]] = []
    for d in bg_docs:
        m = d.metadata or {}
        file_id = m.get("file_id") or m.get("doc_id")
        if not file_id:
            continue

        # ── 2단계: 그 doc 의 clause 청크 전부(조항+메모) ──
        try:
            chunks = await vsm.get_chunks_by_file_id(tenant_id, str(file_id))
        except Exception as e:  # noqa: BLE001
            logger.warning("[/legal-review/precedents] stage2 실패 (%s): %s", file_id, e)
            chunks = []

        clauses: List[Dict[str, Any]] = []
        for ch in chunks:
            cm = ch.get("metadata") or {}
            if cm.get("type") != "clause":
                continue
            clauses.append({
                "clause_no": cm.get("clause_no") or "",
                "clause_title": cm.get("clause_title") or "",
                "topic": cm.get("topic") or "",
                "text": ch.get("content") or "",
                "memos": cm.get("memos") or [],
                "chunk_index": cm.get("chunk_index") or 0,
            })
        clauses.sort(key=lambda c: c.get("chunk_index") or 0)

        profile = m.get("profile") or {}
        precedents.append({
            "file_id": str(file_id),
            "file_name": m.get("file_name") or "",
            "contract_type": m.get("contract_type") or "",
            "profile": profile,
            "background_summary": profile.get("summary") or (d.page_content or ""),
            "clauses": clauses,
        })

    logger.info(
        "[/legal-review/precedents] tenant=%s top_k=%d ctype=%s → %d precedents (clauses=%s)",
        tenant_id, top_k, contract_type, len(precedents),
        [len(p["clauses"]) for p in precedents],
    )
    return {"response": precedents}
