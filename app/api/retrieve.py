"""검색/조회 라우터: /search, /retrieve, /documents/list, /documents/full-text.

벡터 검색은 에이전트가 진입점을 잡는 힌트다(codex 미러의 HINTS.md). 문서를 실제로 읽는
경로는 navigator/folders 라우터의 지도(카드·grep·페이지)이고, 여기는 그 보조다.
"""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from langchain.schema import Document

from app.core.supabase_client import supabase
from app.services.glossary import retrieve_glossary_terms
from app.services.rag_chain import get_rag_chain

router = APIRouter()
logger = logging.getLogger(__name__)


def _summarize_doc(doc: Document, max_chars: int = 200) -> str:
    meta = doc.metadata or {}
    file_name = meta.get("file_name") or meta.get("source") or "?"
    chunk_idx = meta.get("chunk_index") if meta.get("chunk_index") is not None else meta.get("chunk_id") or "?"
    body = (doc.page_content or "").strip().replace("\n", " ")
    if len(body) > max_chars:
        body = body[:max_chars] + "…"
    return f"[{file_name}#{chunk_idx}] {body}"


# 작은 문서로 간주하여 청크 전체를 통째로 컨텍스트에 주입할 임계치(청크 개수)
SMALL_DOC_CHUNK_THRESHOLD = 15


async def _resolve_subtree_file_ids(
    tenant_id: str, folder_paths: List[str]
) -> List[str]:
    """folder_path(들) 의 *subtree* 에 속한 파일들의 source_ref(=file_id) 목록.

    각 폴더 자신 + 하위(``folder_path == p`` 또는 ``folder_path like p/%``). scoped RAG 폴백을
    선택된 폴더 안으로 좁히는 데 쓴다. 대규모 코퍼스 cross-contamination 방어의 핵심.
    """
    out: set[str] = set()
    for raw in folder_paths:
        p = (raw or "").strip().strip("/")
        if not p:
            continue
        try:
            eq = await asyncio.to_thread(
                supabase.table("knowledge_files")
                .select("source_ref")
                .eq("tenant_id", tenant_id)
                .eq("folder_path", p)
                .execute
            )
            for r in (eq.data or []):
                if r.get("source_ref"):
                    out.add(str(r["source_ref"]))
            ch = await asyncio.to_thread(
                supabase.table("knowledge_files")
                .select("source_ref")
                .eq("tenant_id", tenant_id)
                .like("folder_path", f"{p}/%")
                .limit(20_000)
                .execute
            )
            for r in (ch.data or []):
                if r.get("source_ref"):
                    out.add(str(r["source_ref"]))
        except Exception as e:
            logger.warning("[/search] subtree resolve failed for %r: %s", p, e)
    return sorted(out)


@router.get("/search")
async def search(
    query: str,
    tenant_id: str,
    file_ids: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
    top_k: int = Query(default=5, ge=1, le=50),
    exclude_chunk_ids: Optional[List[str]] = Query(default=None),
):
    """엄격한 벡터 검색. ``top_k`` 만큼만 반환. magic 없음.

    필터:
        - ``tenant_id`` 필수
        - ``file_ids`` 옵셔널 — 1개 이상이면 그 파일들 중에서 검색 (``$in``).
          비우면 tenant 전체에서 검색.
        - ``folder_paths`` 옵셔널 — 이 폴더(들)의 subtree 로 검색을 좁힌다. file_ids도 주면
          ``folder subtree ∪ file_ids``. 폴더와 독립 첨부가 공존하는 선택을 보존한다.
        - ``exclude_chunk_ids`` 옵셔널 — 이 chunk_id 들은 결과에서 제외하고 top_k 채움.

    /retrieve 와 달리 small-doc 통째 반환 / glossary merge / room/proc_inst 분기 등
    *암묵적 동작이 일절 없음*. 단일 호출에 단일 top_k — 다중 파일이어도 합쳐서 top_k.
    """
    if not query or not tenant_id:
        raise HTTPException(status_code=400, detail="query, tenant_id required")

    metadata_filter: dict = {"tenant_id": tenant_id}
    cleaned_files = [str(x) for x in (file_ids or []) if x]

    cleaned_folders = [str(x) for x in (folder_paths or []) if x]
    if cleaned_folders:
        subtree_ids = await _resolve_subtree_file_ids(tenant_id, cleaned_folders)
        if cleaned_files:
            # 폴더 subtree ∪ 개별 file_ids — 둘 다 접근(공존 스코프: 폴더 + 방에 올린 파일 등).
            # (예전엔 교집합이라, 폴더와 함께 온 개별 파일이 폴더 밖이면 사라졌음.)
            cleaned_files = sorted(set(subtree_ids) | set(cleaned_files))
        else:
            cleaned_files = subtree_ids
        if not cleaned_files:
            logger.info(
                "[/search] folder_paths=%s → subtree 0 files (no match) → empty result",
                cleaned_folders,
            )
            return {"response": []}

    if cleaned_files:
        metadata_filter["file_id"] = cleaned_files
    excluded = [str(x) for x in (exclude_chunk_ids or []) if x]
    if excluded:
        metadata_filter["_exclude_chunk_ids"] = excluded

    try:
        rag = get_rag_chain()
        result = await rag.retrieve(query, metadata_filter, top_k=top_k)
        docs: List[Document] = (result.get("source_documents") or [])[:top_k]
        logger.info(
            "[/search] tenant=%s file_ids=%s q=%r top_k=%d excluded=%d → %d chunks",
            tenant_id, cleaned_files or None, query[:80], top_k, len(excluded), len(docs),
        )
        return {"response": docs}
    except Exception as e:
        logger.exception("[/search] failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/retrieve")
async def retrieve(
    query: str,
    tenant_id: str,
    proc_inst_id: Optional[str] = None,
    all_docs: bool = False,
    top_k: int = Query(default=5, ge=1, le=100),
    drive_folder_id: Optional[str] = None,
    room_id: Optional[str] = None,
    file_ids: Optional[List[str]] = Query(default=None),
):
    logger.info(
        "[/retrieve] params: query=%r tenant_id=%r room_id=%r file_ids=%s proc_inst_id=%r drive_folder_id=%r all_docs=%s top_k=%d",
        query, tenant_id, room_id, file_ids, proc_inst_id, drive_folder_id, all_docs, top_k,
    )
    try:
        rag = get_rag_chain()
        docs: List[Document] = []
        did_retrieve = False

        # ============================================================
        # 사용자가 명시적으로 선택한 파일들로 좁히는 경로 (Phase 2)
        #   - 작은 문서(청크 ≤ SMALL_DOC_CHUNK_THRESHOLD): 전체 청크 통째로 컨텍스트
        #   - 큰 문서: file_id 단일 필터로 RAG retrieve top_k
        # ============================================================
        unique_file_ids = list(dict.fromkeys([fid for fid in (file_ids or []) if fid]))

        if unique_file_ids:
            from app.services.vector_store import get_vector_store

            vsm = get_vector_store()
            seen_keys: set = set()

            def _push_doc(d: Document) -> None:
                meta = d.metadata or {}
                key = (
                    str(meta.get("file_id") or ""),
                    str(meta.get("chunk_index") or meta.get("chunk_id") or ""),
                )
                if key in seen_keys:
                    return
                seen_keys.add(key)
                docs.append(d)

            for fid in unique_file_ids:
                try:
                    chunks = await vsm.get_chunks_by_file_id(tenant_id, fid)
                except Exception as e:
                    logger.warning("[/retrieve] get_chunks_by_file_id failed for %s: %s", fid, e)
                    chunks = []

                if not chunks:
                    # 청크가 없으면 RAG로라도 시도 (메타 인덱스 누락 등 대비)
                    fallback = await rag.retrieve(
                        query,
                        {"tenant_id": tenant_id, "file_id": fid},
                        top_k=top_k,
                    )
                    for d in (fallback.get("source_documents") or []):
                        _push_doc(d)
                    continue

                if len(chunks) <= SMALL_DOC_CHUNK_THRESHOLD:
                    # 작은 문서: 모든 청크 그대로 사용
                    for c in chunks:
                        _push_doc(Document(page_content=c["content"], metadata=c["metadata"]))
                else:
                    # 큰 문서: file_id 단일 필터로 RAG retrieve
                    sub = await rag.retrieve(
                        query,
                        {"tenant_id": tenant_id, "file_id": fid},
                        top_k=top_k,
                    )
                    for d in (sub.get("source_documents") or []):
                        _push_doc(d)

            did_retrieve = True

        elif room_id:
            room_filter = {"tenant_id": tenant_id, "room_id": room_id}
            global_filter = {"tenant_id": tenant_id, "knowledge_scope": "global"}
            if drive_folder_id:
                room_filter["drive_folder_id"] = drive_folder_id
                global_filter["drive_folder_id"] = drive_folder_id

            room_result = await rag.retrieve(query, room_filter, top_k=top_k)
            global_result = await rag.retrieve(query, global_filter, top_k=top_k)
            # 글로벌 지식을 우선 병합해, 방별 문서가 많아도 글로벌 용어집이 응답 후보에서 밀리지 않도록 한다.
            raw_docs = (global_result.get("source_documents") or []) + (room_result.get("source_documents") or [])
            dedup_keys = set()
            for doc in raw_docs:
                meta = doc.metadata or {}
                dedup_key = (
                    str(meta.get("id") or ""),
                    str(meta.get("chunk_id") or ""),
                    str(meta.get("file_id") or ""),
                    str(meta.get("chunk_index") or ""),
                )
                if dedup_key in dedup_keys:
                    continue
                dedup_keys.add(dedup_key)
                docs.append(doc)
                if len(docs) >= top_k:
                    break
            did_retrieve = True
        elif proc_inst_id:
            metadata_filter = {"tenant_id": tenant_id, "proc_inst_id": proc_inst_id}
        elif all_docs:
            metadata_filter = {"tenant_id": tenant_id}
        else:
            metadata_filter = {"tenant_id": tenant_id, "source_type": "process_output"}

        if not did_retrieve:
            if drive_folder_id:
                metadata_filter = {**metadata_filter, "drive_folder_id": drive_folder_id}

            result = await rag.retrieve(query, metadata_filter, top_k=top_k)
            docs = result["source_documents"]
            if drive_folder_id:
                docs = [
                    doc for doc in docs
                    if (doc.metadata or {}).get("drive_folder_id") == drive_folder_id
                ]

        glossary_docs = await retrieve_glossary_terms(query=query, tenant_id=tenant_id, top_k=top_k)
        if glossary_docs:
            merged_docs: List[Document] = []
            dedup_keys = set()
            for doc in glossary_docs + docs:
                meta = doc.metadata or {}
                dedup_key = (
                    str(meta.get("source_type") or ""),
                    str(meta.get("term_id") or ""),
                    str(meta.get("id") or ""),
                    str(meta.get("chunk_id") or ""),
                    str(meta.get("file_id") or ""),
                    str(meta.get("chunk_index") or ""),
                    (doc.page_content or "").strip(),
                )
                if dedup_key in dedup_keys:
                    continue
                dedup_keys.add(dedup_key)
                merged_docs.append(doc)
                if len(merged_docs) >= max(top_k, min(top_k * 2, 20)):
                    break
            docs = merged_docs

        logger.info("[/retrieve] returned %d chunks", len(docs))
        for i, d in enumerate(docs):
            logger.info("[/retrieve] chunk[%d] %s", i, _summarize_doc(d))

        return {"response": docs}

    except Exception as e:
        logger.exception("[/retrieve] failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/documents/list")
async def list_documents(
    tenant_id: str,
    drive_folder_id: Optional[str] = None,
    include_images: bool = False,
    folder_path: Optional[str] = None,
    recursive: bool = False,
):
    """테넌트의 내부 지식공간 파일 목록을 knowledge_files에서 조회한다.

    - folder_path 미지정: 테넌트 전체(전체 조회 — 대량 테넌트에선 무거움).
    - folder_path 지정: 그 폴더 파일만(lazy 로딩). recursive=True 면 하위 포함.

    응답:
        files: [file_name, ...]                       (역호환)
        file_details: [{file_name, drive_folder_name, ...}, ...]   (확장 메타 포함)
        total: 개수
    """
    try:
        from app.services.knowledge_files import list_for_tenant, list_for_folder

        if folder_path is not None and str(folder_path).strip().strip("/"):
            rows = await list_for_folder(tenant_id, folder_path, recursive)
        else:
            rows = await list_for_tenant(tenant_id)
        if drive_folder_id:
            rows = [r for r in rows if r.get("drive_folder_id") == drive_folder_id]

        def _summary_status(r: dict) -> str:
            """파일별 요약 상태: skipped | done | failed | pending.

            list_for_tenant 가 doc_card 에서 abstract_status/abstract 를 평탄화해 주므로
            여기서는 그 평탄 필드를 직접 읽는다.
            """
            st = r.get("abstract_status")
            if st in ("done", "failed"):
                return st
            # 구버전 데이터(abstract_status 없음) 호환: abstract 유무로 추론.
            if r.get("abstract"):
                return "done"
            # 인덱싱은 끝났는데 abstract 가 없으면 요약 실패로 간주(재요약 대상).
            return "failed" if r.get("index_status") == "indexed" else "pending"

        file_names: List[str] = []
        file_details: List[dict] = []
        for r in rows:
            name = r.get("file_name")
            if not name:
                continue
            mime = r.get("mime_type") or ""
            if not include_images and mime.startswith("image/"):
                continue
            file_names.append(str(name))
            file_details.append({
                # 역호환 필드
                "file_name": name,
                "drive_folder_name": r.get("folder_path") or "",
                # 확장 필드 (프론트 picker에서 사용)
                "source_type": r.get("source_type"),
                "source_ref": r.get("source_ref"),
                "folder_path": r.get("folder_path") or "",
                "path": r.get("path") or "",
                "drive_folder_id": r.get("drive_folder_id"),
                "mime_type": mime,
                "size_bytes": r.get("size_bytes"),
                "modified_time": r.get("modified_time"),
                "owner": r.get("owner"),
                "uploaded_by_uid": r.get("uploaded_by_uid"),
                "uploaded_by_name": r.get("uploaded_by_name"),
                "index_status": r.get("index_status"),
                "index_error": r.get("index_error"),
                "indexed_at": r.get("indexed_at"),
                "updated_at": r.get("updated_at"),
                # 요약 상태 (프론트 요약 인디케이터/재요약 버튼용)
                "summary_status": _summary_status(r),
            })

        return {
            "files": file_names,
            "file_details": file_details,
            "total": len(file_details),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/documents/full-text")
async def get_full_text(
    tenant_id: str,
    file_ids: List[str] = Query(...),
):
    """파일별 *원본 통째 텍스트* 반환 — RAG chunking 우회.

    documents 테이블의 chunk 들을 file_id 별로 모아 ``chunk_index`` 순으로 concat.

    *용도*: deepagents-lite 의 MOU 작성 흐름처럼 *작은 자료(사업개요)는 통째 컨텍스트*
    를 원할 때. 호출자가 chunks 합치는 로직 복제하지 않게 서버에서 한 번에 제공.

    Response:
        ``{file_id: {file_name, full_text, chunk_count, total_chars}}``
    """
    if not tenant_id or not file_ids:
        raise HTTPException(status_code=400, detail="tenant_id 와 file_ids 필수")

    cleaned = [str(x).strip() for x in file_ids if x and str(x).strip()]
    if not cleaned:
        return {}

    out: dict = {}
    for fid in cleaned:
        try:
            response = (
                supabase.table("documents")
                .select("content, metadata")
                .eq("metadata->>tenant_id", tenant_id)
                .eq("metadata->>file_id", fid)
                .limit(2000)
                .execute()
            )
            rows = response.data or []
        except Exception as exc:
            logger.exception("[/documents/full-text] DB 조회 실패 file_id=%s: %s", fid, exc)
            out[fid] = {"file_name": "", "full_text": "", "chunk_count": 0,
                        "total_chars": 0, "error": str(exc)}
            continue

        # image_analysis 등 메타 chunk 제외
        chunks = []
        file_name = ""
        for row in rows:
            meta = row.get("metadata") or {}
            if meta.get("type") == "image_analysis":
                continue
            if not file_name:
                file_name = meta.get("file_name") or meta.get("source") or ""
            chunks.append({
                "content": row.get("content") or "",
                "chunk_index": int(meta.get("chunk_index") or 0),
            })

        # chunk_index 순 정렬 + concat
        chunks.sort(key=lambda c: c["chunk_index"])
        full_text = "\n".join(c["content"] for c in chunks if c["content"]).strip()

        out[fid] = {
            "file_name": file_name,
            "full_text": full_text,
            "chunk_count": len(chunks),
            "total_chars": len(full_text),
        }

    logger.info(
        "[/documents/full-text] tenant=%s files=%d → sizes=%s",
        tenant_id, len(out),
        {fid: meta.get("total_chars") for fid, meta in out.items()},
    )
    return out

