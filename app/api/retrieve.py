"""검색/조회 라우터: /retrieve, /retrieve-images, /retrieve-by-indices, /documents/*, /preview/pdf-highlight."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from langchain.schema import Document

from app.core.supabase_client import supabase
from app.schemas import RetrieveByIndicesRequest
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


@router.get("/search")
async def search(
    query: str,
    tenant_id: str,
    file_ids: Optional[List[str]] = Query(default=None),
    top_k: int = Query(default=5, ge=1, le=50),
    exclude_chunk_ids: Optional[List[str]] = Query(default=None),
):
    """엄격한 벡터 검색. ``top_k`` 만큼만 반환. magic 없음.

    필터:
        - ``tenant_id`` 필수
        - ``file_ids`` 옵셔널 — 1개 이상이면 그 파일들 중에서 검색 (``$in``).
          비우면 tenant 전체에서 검색.
        - ``exclude_chunk_ids`` 옵셔널 — 이 chunk_id 들은 결과에서 제외하고 top_k 채움.

    /retrieve 와 달리 small-doc 통째 반환 / glossary merge / room/proc_inst 분기 등
    *암묵적 동작이 일절 없음*. 단일 호출에 단일 top_k — 다중 파일이어도 합쳐서 top_k.
    """
    if not query or not tenant_id:
        raise HTTPException(status_code=400, detail="query, tenant_id required")

    metadata_filter: dict = {"tenant_id": tenant_id}
    cleaned_files = [str(x) for x in (file_ids or []) if x]
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


@router.get("/retrieve-images")
async def retrieve_images(
    query: str,
    tenant_id: str,
    top_k: int = Query(default=5, ge=1, le=30),
    drive_folder_id: Optional[str] = None,
):
    """캡션(Vision 분석) 기반 이미지 전용 검색."""
    try:
        metadata_filter = {"tenant_id": tenant_id, "type": "image_analysis"}
        if drive_folder_id:
            metadata_filter["drive_folder_id"] = drive_folder_id

        rag = get_rag_chain()
        result = await rag.retrieve(query, metadata_filter, top_k=top_k)
        docs = result["source_documents"]

        if drive_folder_id:
            docs = [
                doc for doc in docs
                if (doc.metadata or {}).get("drive_folder_id") == drive_folder_id
            ]

        images = []
        for doc in docs:
            meta = doc.metadata or {}
            images.append({
                "image_id": meta.get("image_id", ""),
                "image_url": meta.get("image_url", ""),
                "caption": (doc.page_content or "").strip(),
                "file_name": meta.get("file_name", ""),
                "source_file_name": meta.get("source_file_name", ""),
                "drive_folder_name": meta.get("drive_folder_name", ""),
                "metadata": meta,
            })
        return {"images": images}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/documents/chunks-metadata")
async def get_chunks_metadata(tenant_id: str, file_name: str, drive_folder_id: Optional[str] = None):
    """특정 문서의 모든 청크 메타데이터를 반환."""
    try:
        from app.services.vector_store import get_vector_store
        vsm = get_vector_store()
        chunks = await vsm.get_all_chunks_metadata(
            tenant_id=tenant_id,
            file_name=file_name,
            drive_folder_id=drive_folder_id,
        )
        return {"file_name": file_name, "total_chunks": len(chunks), "chunks": chunks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/documents/list")
async def list_documents(
    tenant_id: str,
    drive_folder_id: Optional[str] = None,
    include_images: bool = False,
):
    """테넌트의 내부 지식공간 파일 목록을 knowledge_files에서 조회한다.

    응답:
        files: [file_name, ...]                       (역호환)
        file_details: [{file_name, drive_folder_name, ...}, ...]   (확장 메타 포함)
        total: 개수
    """
    try:
        from app.services.knowledge_files import list_for_tenant

        rows = await list_for_tenant(tenant_id)
        if drive_folder_id:
            rows = [r for r in rows if r.get("drive_folder_id") == drive_folder_id]

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
                "doc_role": r.get("doc_role") or "content",
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


@router.get("/documents/chunks-by-file-path")
async def get_chunks_by_file_path(
    tenant_id: str,
    file_path: str,
    room_id: Optional[str] = None,
    limit: int = Query(default=200, ge=1, le=2000),
):
    """특정 storage file_path의 청크 본문/메타를 반환한다."""
    try:
        query = (
            supabase.table("documents")
            .select("content, metadata")
            .eq("metadata->>tenant_id", tenant_id)
            .eq("metadata->>file_path", file_path)
            .limit(limit)
        )
        if room_id:
            query = query.eq("metadata->>room_id", room_id)

        response = query.execute()
        rows = response.data or []

        chunks = []
        for row in rows:
            meta = row.get("metadata") or {}
            if meta.get("type") == "image_analysis":
                continue
            if room_id and str(meta.get("room_id") or "") != str(room_id):
                continue
            chunks.append(
                {
                    "page_content": row.get("content") or "",
                    "metadata": meta,
                }
            )

        chunks.sort(key=lambda c: int((c.get("metadata") or {}).get("chunk_index") or 0))
        return {
            "tenant_id": tenant_id,
            "file_path": file_path,
            "room_id": room_id,
            "total_chunks": len(chunks),
            "chunks": chunks,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _candidate_file_paths(file_path: str) -> List[str]:
    """
    메멘토는 storage 업로드 시 `files/{uuid}.{ext}` 형태로 file_path를 저장한다.
    하지만 work-assistant-agent MCP는 `/files/` 마커 이후 부분만 잘라
    `{uuid}.{ext}` 형태로 path를 전달하기도 한다.
    두 형태 모두에서 청크를 찾을 수 있도록 후보 경로를 생성한다.
    """
    raw = (file_path or "").strip().rstrip("?")
    if not raw:
        return []

    candidates: List[str] = [raw]
    if not raw.startswith("files/"):
        candidates.append(f"files/{raw.lstrip('/')}")
    elif raw.startswith("files/"):
        candidates.append(raw[len("files/"):])

    # dedupe but preserve order
    seen: set = set()
    out: List[str] = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


@router.get("/documents/chunks-with-embeddings")
async def get_chunks_with_embeddings(
    tenant_id: str,
    file_path: Optional[str] = None,
    file_name: Optional[str] = None,
    room_id: Optional[str] = None,
    include_embeddings: bool = True,
    limit: int = Query(default=2000, ge=1, le=5000),
):
    """
    pdf2bpmn 등 다운스트림 에이전트가 메멘토의 청크와 임베딩을 그대로 재사용할 수 있도록
    file_path 또는 file_name 기준으로 (1) Supabase documents의 본문/메타와
    (2) Chroma에 저장된 임베딩 벡터를 합쳐서 반환한다.

    매칭 우선순위:
      1) file_path (memento storage 경로). `files/` prefix 유무에 모두 대응.
      2) file_name (동일 tenant 내에서 unique한 케이스가 일반적).

    응답 형식:
      {
        "tenant_id", "file_path", "file_name", "room_id",
        "total_chunks", "embedding_count",
        "chunks": [{"page_content", "metadata", "embedding"|null}, ...]
      }
    """
    if not file_path and not file_name:
        raise HTTPException(status_code=400, detail="file_path 또는 file_name 중 하나는 필수입니다.")

    try:
        rows: List[dict] = []

        if file_path:
            for candidate in _candidate_file_paths(file_path):
                query = (
                    supabase.table("documents")
                    .select("id, content, metadata")
                    .eq("metadata->>tenant_id", tenant_id)
                    .eq("metadata->>file_path", candidate)
                    .limit(limit)
                )
                if room_id:
                    query = query.eq("metadata->>room_id", room_id)
                response = query.execute()
                if response.data:
                    rows = response.data
                    break

        if not rows and file_name:
            query = (
                supabase.table("documents")
                .select("id, content, metadata")
                .eq("metadata->>tenant_id", tenant_id)
                .eq("metadata->>file_name", file_name)
                .limit(limit)
            )
            if room_id:
                query = query.eq("metadata->>room_id", room_id)
            response = query.execute()
            rows = response.data or []

        text_rows: List[dict] = []
        for row in rows:
            meta = row.get("metadata") or {}
            if meta.get("type") == "image_analysis":
                continue
            if room_id and str(meta.get("room_id") or "") != str(room_id):
                continue
            text_rows.append(row)

        text_rows.sort(key=lambda c: int((c.get("metadata") or {}).get("chunk_index") or 0))

        embeddings_map: dict = {}
        if include_embeddings and text_rows:
            row_ids = [str(r.get("id") or "") for r in text_rows if r.get("id")]
            if row_ids:
                try:
                    from app.services.vector_store import get_vector_store

                    vsm = get_vector_store()
                    fetched = await asyncio.to_thread(
                        vsm.collection.get,
                        ids=row_ids,
                        include=["embeddings"],
                    )
                    _f_ids = fetched.get("ids")
                    fetched_ids = list(_f_ids) if _f_ids is not None else []
                    _f_embs = fetched.get("embeddings")
                    fetched_embs = list(_f_embs) if _f_embs is not None else []
                    for i, rid in enumerate(fetched_ids):
                        if i < len(fetched_embs):
                            emb = fetched_embs[i]
                            if emb is not None:
                                try:
                                    embeddings_map[str(rid)] = list(emb)
                                except Exception:
                                    embeddings_map[str(rid)] = None
                except Exception as e:
                    logger.warning("[chunks-with-embeddings] Chroma 임베딩 조회 실패: %s", e)

        chunks: List[dict] = []
        for row in text_rows:
            rid = str(row.get("id") or "")
            chunk_payload = {
                "page_content": row.get("content") or "",
                "metadata": row.get("metadata") or {},
            }
            if include_embeddings:
                chunk_payload["embedding"] = embeddings_map.get(rid)
            chunks.append(chunk_payload)

        embedding_count = sum(1 for c in chunks if c.get("embedding"))
        logger.info(
            "[/documents/chunks-with-embeddings] tenant=%r file_path=%r file_name=%r "
            "room_id=%r → chunks=%d embeddings=%d",
            tenant_id, file_path, file_name, room_id, len(chunks), embedding_count,
        )

        return {
            "tenant_id": tenant_id,
            "file_path": file_path,
            "file_name": file_name,
            "room_id": room_id,
            "total_chunks": len(chunks),
            "embedding_count": embedding_count,
            "chunks": chunks,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("[/documents/chunks-with-embeddings] failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/documents/chunks-by-file-name")
async def get_chunks_by_file_name(
    tenant_id: str,
    file_name: str,
    room_id: Optional[str] = None,
    limit: int = Query(default=200, ge=1, le=2000),
):
    """특정 file_name의 청크 본문/메타를 반환한다."""
    try:
        query = (
            supabase.table("documents")
            .select("content, metadata")
            .eq("metadata->>tenant_id", tenant_id)
            .eq("metadata->>file_name", file_name)
            .limit(limit)
        )
        if room_id:
            query = query.eq("metadata->>room_id", room_id)

        response = query.execute()
        rows = response.data or []

        chunks = []
        for row in rows:
            meta = row.get("metadata") or {}
            if meta.get("type") == "image_analysis":
                continue
            if room_id and str(meta.get("room_id") or "") != str(room_id):
                continue
            chunks.append(
                {
                    "page_content": row.get("content") or "",
                    "metadata": meta,
                }
            )

        chunks.sort(key=lambda c: int((c.get("metadata") or {}).get("chunk_index") or 0))
        return {
            "tenant_id": tenant_id,
            "file_name": file_name,
            "room_id": room_id,
            "total_chunks": len(chunks),
            "chunks": chunks,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/preview/pdf-highlight")
async def preview_pdf_highlight(
    tenant_id: str,
    file_id: str,
    page: int,
    bbox: str,
    dpi: int = 150,
):
    """PDF 한 페이지 + bbox 하이라이트를 PNG로 렌더링해 Supabase에 캐시 후 public URL 반환."""
    import pymupdf

    try:
        bbox_parts = [float(v.strip()) for v in bbox.split(",")]
        if len(bbox_parts) != 4:
            raise ValueError("bbox must be 'x0,y0,x1,y1'")
        x0, y0, x1, y1 = bbox_parts
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid bbox: {exc}")
    if not file_id:
        raise HTTPException(status_code=400, detail="file_id required")

    cache_key_src = f"{file_id}|{page}|{x0:.2f},{y0:.2f},{x1:.2f},{y1:.2f}|dpi={dpi}"
    cache_key = hashlib.sha1(cache_key_src.encode("utf-8")).hexdigest()
    cache_path = f"pdf-highlight-cache/{tenant_id}/{cache_key}.png"

    try:
        existing = await asyncio.to_thread(
            supabase.storage.from_("files").download, cache_path
        )
        if existing:
            public_url_resp = supabase.storage.from_("files").get_public_url(cache_path)
            cached_url = (
                public_url_resp.get("publicURL", "")
                if isinstance(public_url_resp, dict) else str(public_url_resp)
            )
            if cached_url:
                return {
                    "url": cached_url,
                    "cache_key": cache_key,
                    "page": page,
                    "cached": True,
                }
    except Exception:
        pass

    try:
        pdf_bytes = await asyncio.to_thread(
            supabase.storage.from_("files").download, file_id
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"PDF not found in storage: {file_id} ({exc})")
    if not pdf_bytes:
        raise HTTPException(status_code=404, detail=f"PDF empty: {file_id}")

    def _render() -> tuple[bytes, int, int]:
        pdf = pymupdf.open(stream=pdf_bytes, filetype="pdf")
        try:
            if page < 0 or page >= pdf.page_count:
                raise ValueError(f"page {page} out of range (0..{pdf.page_count - 1})")
            pg = pdf.load_page(page)
            rect = pymupdf.Rect(x0, y0, x1, y1)
            rect = rect & pg.rect
            pg.draw_rect(
                rect,
                color=(1, 0.75, 0),
                fill=(1, 0.92, 0.2),
                fill_opacity=0.35,
                width=1.5,
            )
            pix = pg.get_pixmap(dpi=dpi)
            return pix.tobytes("png"), pix.width, pix.height
        finally:
            pdf.close()

    try:
        png_bytes, img_w, img_h = await asyncio.to_thread(_render)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"render failed: {exc}")

    try:
        await asyncio.to_thread(
            supabase.storage.from_("files").upload,
            cache_path,
            png_bytes,
            {"content-type": "image/png", "upsert": "true"},
        )
    except Exception as exc:
        print(f"[pdf-highlight] 캐시 업로드 실패: {exc}")

    public_url_resp = supabase.storage.from_("files").get_public_url(cache_path)
    public_url = (
        public_url_resp.get("publicURL", "")
        if isinstance(public_url_resp, dict) else str(public_url_resp)
    )
    return {
        "url": public_url,
        "cache_key": cache_key,
        "page": page,
        "width": img_w,
        "height": img_h,
        "cached": False,
    }


@router.post("/retrieve-by-indices")
async def retrieve_by_indices(request: RetrieveByIndicesRequest):
    """LLM이 선택한 chunk_index 리스트로 청크를 직접 조회."""
    try:
        from app.services.vector_store import get_vector_store
        vsm = get_vector_store()
        docs = await vsm.get_chunks_by_indices(
            tenant_id=request.tenant_id,
            file_name=request.file_name,
            chunk_indices=request.chunk_indices,
            drive_folder_id=request.drive_folder_id,
        )
        return {
            "response": [
                {"page_content": doc.page_content, "metadata": doc.metadata}
                for doc in docs
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
