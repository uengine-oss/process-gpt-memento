"""섹션 검색 — 키워드(Postgres)와 벡터(섹션 전용 컬렉션)를 섹션 단위로 합친다.

청크 힌트 인덱스(`/search`)와 섞이지 않도록 섹션 벡터는 별도 컬렉션에 둔다.
계약: docs/specs/knowledge-map.md#섹션-검색
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

SECTION_COLLECTION = "kb_sections"
# 임베딩 입력 상한 — 섹션 앞부분(제목·요약·본문 시작)이 검색 표면이다.
EMBED_CHARS = 4000
RRF_K = 60
_JOSA = ("에서는", "으로는", "에서", "으로", "에게", "까지", "부터", "은", "는", "이", "가", "을", "를",
         "의", "에", "로", "와", "과", "도", "만")

_index = None


def _section_index():
    global _index
    if _index is None:
        from app.services.vector_index import make_vector_index

        _index = make_vector_index(SECTION_COLLECTION)
    return _index


def query_terms(query: str) -> List[str]:
    """질문 → 부분 일치 검색어. 한국어 조사를 뗀 형태도 함께 넣는다."""
    terms: List[str] = []
    for token in re.findall(r"[0-9A-Za-z가-힣]+", query or ""):
        candidates = [token]
        for josa in _JOSA:
            if len(token) - len(josa) >= 2 and token.endswith(josa):
                candidates.append(token[: -len(josa)])
                break
        for term in candidates:
            if len(term) >= 2 and term.casefold() not in {t.casefold() for t in terms}:
                terms.append(term)
    return terms[:12]


def section_text(section: Dict[str, Any], blocks: Sequence[Dict[str, Any]]) -> str:
    return "\n".join(blocks[i]["text"] for i in range(section["start"], section["end"] + 1))


async def index_sections(
    tenant_id: str, file_id: str, file_name: str,
    sections: Sequence[Dict[str, Any]], blocks: Sequence[Dict[str, Any]],
) -> int:
    """섹션을 벡터 컬렉션에 파일 단위로 교체한다. 실패는 격리 — 키워드 검색은 계속 된다."""
    if not sections:
        return 0
    try:
        from app.services.vector_store import get_vector_store

        index = _section_index()
        texts = [
            f"{file_name} › {s['title']}\n{s.get('summary') or ''}\n{section_text(s, blocks)}"[:EMBED_CHARS]
            for s in sections
        ]
        embeddings = await asyncio.to_thread(get_vector_store()._embed_texts, texts)
        await asyncio.to_thread(index.delete_where, {"$and": [{"tenant_id": tenant_id}, {"file_id": file_id}]})
        ids = [f"{tenant_id}:{file_id}:{s['section_index']}" for s in sections]
        metadatas = [{"tenant_id": tenant_id, "file_id": file_id, "section_index": int(s["section_index"])}
                     for s in sections]
        await asyncio.to_thread(index.upsert, ids, embeddings, texts, metadatas)
        return len(ids)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[section_search] vector index failed (%s/%s): %s", tenant_id, file_id, exc)
        return 0


async def reindex_file(tenant_id: str, file_id: str, file_name: str) -> int:
    """저장된 섹션·블록으로 섹션 벡터를 다시 만든다(카드 재사용·백필 경로)."""
    from app.core.supabase_client import supabase
    from app.services.doc_sections import load_blocks

    result = await asyncio.to_thread(
        supabase.table("document_sections").select("*").eq("tenant_id", tenant_id)
        .eq("file_id", file_id).order("section_index").execute
    )
    sections = [
        {"section_index": r["section_index"], "start": r["start_block"], "end": r["end_block"],
         "title": r["title"], "summary": r.get("summary") or ""}
        for r in (getattr(result, "data", None) or [])
    ]
    blocks = await load_blocks(tenant_id, file_id)
    if not sections or not blocks:
        return 0
    return await index_sections(tenant_id, file_id, file_name, sections, blocks)


async def forget_files(tenant_id: str, file_ids: Sequence[str]) -> None:
    """파일 삭제·재인덱싱 때 블록·섹션·섹션 벡터를 함께 지운다. 실패는 경고만."""
    from app.core.supabase_client import supabase

    ids = [str(f) for f in file_ids if f]
    for start in range(0, len(ids), 100):
        batch = ids[start:start + 100]
        for table in ("document_sections", "document_blocks"):
            try:
                await asyncio.to_thread(
                    supabase.table(table).delete().eq("tenant_id", tenant_id).in_("file_id", batch).execute
                )
            except Exception as exc:  # noqa: BLE001 - 테이블 미배포 등
                logger.warning("[section_search] %s cleanup failed: %s", table, exc)
        try:
            await asyncio.to_thread(
                _section_index().delete_where,
                {"$and": [{"tenant_id": tenant_id}, {"file_id": {"$in": batch}}]},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[section_search] vector cleanup failed: %s", exc)


async def _keyword(tenant_id: str, file_ids: List[str], terms: List[str], limit: int) -> List[Dict[str, Any]]:
    from app.core.supabase_client import supabase

    if not terms:
        return []
    result = await asyncio.to_thread(
        supabase.rpc("kb_section_keyword_search", {
            "p_tenant_id": tenant_id, "p_file_ids": file_ids, "p_terms": terms, "p_limit": limit,
        }).execute
    )
    return list(getattr(result, "data", None) or [])


async def _vector(tenant_id: str, file_ids: List[str], query: str, limit: int) -> List[Dict[str, Any]]:
    from app.services.vector_store import get_vector_store

    embedding = (await asyncio.to_thread(get_vector_store()._embed_texts, [query]))[0]
    where = {"$and": [{"tenant_id": tenant_id}, {"file_id": {"$in": file_ids}}]}
    hits = await asyncio.to_thread(_section_index().query, embedding, limit, where, True)
    out = []
    for hit in hits or []:
        meta = hit.get("metadata") if isinstance(hit, dict) else None
        if meta:
            out.append({"file_id": meta["file_id"], "section_index": int(meta["section_index"]),
                        "distance": hit.get("distance")})
    return out


async def search(
    tenant_id: str, file_ids: List[str], query: str, top_k: int = 20,
) -> Dict[str, Any]:
    """두 검색의 순위를 RRF 로 합친다. 한쪽이 실패해도 다른 쪽 결과로 답한다."""
    terms = query_terms(query)
    pool = max(top_k * 3, 30)
    keyword_task = asyncio.create_task(_keyword(tenant_id, file_ids, terms, pool))
    vector_task = asyncio.create_task(_vector(tenant_id, file_ids, query, pool))
    keyword, vector = await asyncio.gather(keyword_task, vector_task, return_exceptions=True)
    errors = {}
    if isinstance(keyword, Exception):
        errors["keyword"] = f"{type(keyword).__name__}: {keyword}"[:200]
        keyword = []
    if isinstance(vector, Exception):
        errors["vector"] = f"{type(vector).__name__}: {vector}"[:200]
        vector = []
    fused: Dict[tuple, Dict[str, Any]] = {}
    for source, rows in (("keyword", keyword), ("vector", vector)):
        for rank, row in enumerate(rows, start=1):
            key = (row["file_id"], int(row["section_index"]))
            item = fused.setdefault(key, {"file_id": key[0], "section_index": key[1], "score": 0.0,
                                          "matched_terms": [], "ranks": {}})
            item["score"] += 1.0 / (RRF_K + rank)
            item["ranks"][source] = rank
            if source == "keyword":
                item["matched_terms"] = row.get("matched_terms") or []
    ranked = sorted(fused.values(), key=lambda item: -item["score"])[:top_k]
    return {"terms": terms, "results": ranked, "errors": errors}


async def load_sections(tenant_id: str, keys: Sequence[tuple]) -> Dict[tuple, Dict[str, Any]]:
    from app.core.supabase_client import supabase

    by_file: Dict[str, List[int]] = {}
    for file_id, index in keys:
        by_file.setdefault(file_id, []).append(index)
    out: Dict[tuple, Dict[str, Any]] = {}
    for file_id, indexes in by_file.items():
        result = await asyncio.to_thread(
            supabase.table("document_sections").select("*")
            .eq("tenant_id", tenant_id).eq("file_id", file_id).in_("section_index", indexes).execute
        )
        for row in getattr(result, "data", None) or []:
            out[(file_id, row["section_index"])] = row
    return out


async def load_block_range(tenant_id: str, file_id: str, start: int, end: int) -> List[Dict[str, Any]]:
    from app.core.supabase_client import supabase

    result = await asyncio.to_thread(
        supabase.table("document_blocks").select("block_index, kind, text, heading_level, page_number")
        .eq("tenant_id", tenant_id).eq("file_id", file_id)
        .gte("block_index", start).lte("block_index", end).order("block_index").execute
    )
    return list(getattr(result, "data", None) or [])


def snippet(text: str, terms: Sequence[str], width: int = 240) -> str:
    lowered = text.casefold()
    positions = [lowered.find(t.casefold()) for t in terms if lowered.find(t.casefold()) >= 0]
    start = max(0, (min(positions) if positions else 0) - 60)
    return " ".join(text[start:start + width].split())


def pages_of(blocks: Sequence[Dict[str, Any]]) -> Optional[List[int]]:
    pages = sorted({b["page_number"] for b in blocks if b.get("page_number")})
    return [pages[0], pages[-1]] if pages else None
