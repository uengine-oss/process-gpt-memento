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


@router.get("/retrieve")
async def retrieve(
    query: str,
    tenant_id: str,
    proc_inst_id: Optional[str] = None,
    all_docs: bool = False,
    top_k: int = Query(default=5, ge=1, le=100),
    drive_folder_id: Optional[str] = None,
    room_id: Optional[str] = None,
    file_name: Optional[str] = None,
):
    logger.info(
        "[/retrieve] params: query=%r tenant_id=%r room_id=%r file_name=%r proc_inst_id=%r drive_folder_id=%r all_docs=%s top_k=%d",
        query, tenant_id, room_id, file_name, proc_inst_id, drive_folder_id, all_docs, top_k,
    )
    try:
        rag = get_rag_chain()
        docs: List[Document] = []
        did_retrieve = False

        if room_id and file_name:
            # room + file_name 조합: 글로벌 머지 없이 정확히 그 파일만
            metadata_filter = {"tenant_id": tenant_id, "room_id": room_id, "file_name": file_name}
            if drive_folder_id:
                metadata_filter["drive_folder_id"] = drive_folder_id
            result = await rag.retrieve(query, metadata_filter, top_k=top_k)
            docs = result.get("source_documents") or []
            did_retrieve = True
        elif file_name:
            # 파일명 단독 필터: 테넌트 + file_name
            metadata_filter = {"tenant_id": tenant_id, "file_name": file_name}
            if drive_folder_id:
                metadata_filter["drive_folder_id"] = drive_folder_id
            result = await rag.retrieve(query, metadata_filter, top_k=top_k)
            docs = result.get("source_documents") or []
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
    """특정 폴더(옵션) 내 문서명 목록을 반환."""
    try:
        query = (
            supabase.table("documents")
            .select("metadata")
            .eq("metadata->>tenant_id", tenant_id)
        )
        if drive_folder_id:
            query = query.eq("metadata->>drive_folder_id", drive_folder_id)
        response = query.execute()

        file_names: List[str] = []
        file_folder_map: dict = {}
        for row in response.data or []:
            meta = row.get("metadata") or {}
            if not include_images and meta.get("type") == "image_analysis":
                continue
            name = meta.get("file_name")
            if name:
                name = str(name)
                file_names.append(name)
                if name not in file_folder_map:
                    folder = meta.get("drive_folder_name") or ""
                    file_folder_map[name] = str(folder)

        unique_files = list(dict.fromkeys(file_names))
        file_details = [
            {"file_name": f, "drive_folder_name": file_folder_map.get(f, "")}
            for f in unique_files
        ]
        return {"files": unique_files, "file_details": file_details, "total": len(unique_files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
                    fetched_ids = list(fetched.get("ids") or [])
                    fetched_embs = list(fetched.get("embeddings") or [])
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
