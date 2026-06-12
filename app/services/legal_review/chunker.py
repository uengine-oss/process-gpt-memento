"""구조화 결과 → 벡터 인덱싱용 Document 청크.

청크 2종:
  ① 사업배경 (문서당 1개)  — 검색 키. page_content = 정규화 사업배경 텍스트.
  ② 조항 (문서당 N개)       — page_content = topic+제목+본문. *메모는 metadata 로 동승*.

metadata 규약 (deepagents-lite 2단계 검색이 의존):
  공통: doc_role='legal_review', contract_type, file_id, doc_id(=file_id), file_name,
        tenant_id, knowledge_scope='global', language
  ①  : type='background', profile(dict)
  ②  : type='clause', clause_no, clause_title, topic, has_memo(bool), memos(list)

검색 흐름:
  1단계: filter {doc_role:legal_review, type:background} + 새 계약 사업배경 임베딩 → top-K doc_id
  2단계: filter {doc_role:legal_review, type:clause, doc_id∈top-K} → 조항+메모 회수
"""
from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List

from langchain.schema import Document

from app.services.legal_review.structurer import structure_contract


def _bg_embed_text(profile: Dict[str, Any], file_name: str) -> str:
    """사업배경 profile → 임베딩용 합성 텍스트 (쿼리·코퍼스 동일 방식)."""
    parts = [
        profile.get("industry", ""),
        profile.get("subject", ""),
        profile.get("summary", ""),
        " ".join(profile.get("key_terms") or []),
    ]
    return " | ".join(x for x in parts if x).strip() or file_name


def build_documents_from_structured(
    result: Dict[str, Any], file_id: str, file_name: str, tenant_id: str
) -> List[Document]:
    profile = (result.get("business_background") or {}).get("profile") or {}
    base = {
        "doc_role": "legal_review",
        "contract_type": result.get("contract_type", "other"),
        "language": result.get("language", ""),
        "file_id": file_id,
        "doc_id": file_id,
        "file_name": file_name,
        "tenant_id": tenant_id,
        "knowledge_scope": "global",
        "storage_type": "storage",
        "source": "legal_review",
    }

    docs: List[Document] = [
        Document(
            page_content=_bg_embed_text(profile, file_name),
            metadata={**base, "type": "background", "chunk_index": 0, "profile": profile},
        )
    ]
    for i, c in enumerate(result.get("clauses") or [], start=1):
        memos = c.get("memos") or []
        title = c.get("title", "")
        topic = c.get("topic", "")
        body = c.get("text", "")
        page = f"[{topic}] {title}\n{body}".strip()
        docs.append(
            Document(
                page_content=page,
                metadata={
                    **base,
                    "type": "clause",
                    "chunk_index": i,
                    "clause_no": c.get("clause_no", ""),
                    "clause_title": title,
                    "topic": topic,
                    "has_memo": bool(memos),
                    "memos": memos,
                },
            )
        )
    return docs


def build_legal_review_documents(
    file_content: bytes, file_name: str, file_id: str, tenant_id: str
) -> List[Document]:
    """docx 바이트 → 구조화 → Document 청크 리스트. 동기(호출부에서 to_thread)."""
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
            tmp.write(file_content)
            tmp_path = tmp.name
        result = structure_contract(tmp_path)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    return build_documents_from_structured(result, file_id, file_name, tenant_id)
