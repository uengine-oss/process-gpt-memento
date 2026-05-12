"""내부 지식공간 파일 관리 API — 설정 페이지에서 직접 업로드/삭제."""
from __future__ import annotations

import hashlib
import io
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

from app.services.document_processor import get_document_processor
from app.services.knowledge_files import (
    INDEX_STATUS_FAILED,
    INDEX_STATUS_INDEXED,
    create_folder as kf_create_folder,
    delete_entry,
    delete_folder_meta,
    find_by_hash,
    get_entry,
    list_files_in_folder_recursive,
    list_folders_for_tenant,
    mark_status,
    register_uploaded_file,
    rename_folder,
    sanitize_storage_folder_path,
)
from app.services.rag_chain import get_rag_chain
from app.storage.supabase_loader import SupabaseStorageLoader

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/knowledge/files/check-hash")
async def check_knowledge_file_hash(
    tenant_id: str = Query(...),
    file_hash: str = Query(..., min_length=64, max_length=64),
):
    """클라이언트가 업로드 직전에 호출 — 동일 테넌트 내 같은 해시 파일 존재 여부 반환.

    존재하면 기존 파일 메타를 돌려줘서 사용자에게 "이미 올라간 파일" 알림을 띄울 수 있게 한다.
    """
    existing = await find_by_hash(tenant_id=tenant_id, file_hash=file_hash.lower())
    return {"exists": bool(existing), "existing": existing}


@router.post("/knowledge/files/upload")
async def upload_knowledge_file(
    file: UploadFile = File(...),
    tenant_id: str = Form(...),
    folder_path: Optional[str] = Form(None),
    file_hash: Optional[str] = Form(None),
    uploaded_by_uid: Optional[str] = Form(None),
    uploaded_by_name: Optional[str] = Form(None),
):
    """설정 페이지에서 내부 지식공간 파일을 직접 업로드.

    실물 파일은 Supabase Storage 'files' 버킷의 'knowledge/{tenant}/' 아래에 저장하고,
    knowledge_files 테이블에 source_type='upload'로 등록한 후 RAG 인덱싱한다.
    """
    file_content = await file.read()
    file_name = file.filename or "unknown"
    size_bytes = len(file_content)

    # SHA-256 해시 — 클라이언트가 보낸 값을 신뢰하지 않고 서버에서 재계산해 검증/저장
    computed_hash = hashlib.sha256(file_content).hexdigest()
    if file_hash and file_hash.lower() != computed_hash:
        logger.warning(
            "[knowledge_admin] client hash mismatch (%s != %s) — using server-computed",
            file_hash, computed_hash,
        )
    file_hash = computed_hash

    # 1) Storage 업로드 — folder_path를 storage 경로에도 반영 (ASCII-safe로 변환)
    storage_loader = SupabaseStorageLoader()
    raw_folder_path = (folder_path or "").strip().strip("/")
    safe_folder_segment = sanitize_storage_folder_path(raw_folder_path)
    folder = (
        f"knowledge/{tenant_id}/{safe_folder_segment}"
        if safe_folder_segment
        else f"knowledge/{tenant_id}"
    )
    try:
        upload_result = await storage_loader.upload_file_to_storage(
            file_content, file_name, folder_path=folder
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Storage upload failed: {e}")

    storage_path = upload_result["file_path"]
    public_url = upload_result.get("public_url")

    # 2) knowledge_files 사전 등록 (processing)
    await register_uploaded_file(
        tenant_id=tenant_id,
        storage_path=storage_path,
        file_name=file_name,
        folder_path=folder_path or "",
        mime_type=file.content_type or "",
        size_bytes=size_bytes,
        file_hash=file_hash,
        uploaded_by_uid=(uploaded_by_uid or None),
        uploaded_by_name=(uploaded_by_name or None),
    )

    # 3) 콘텐츠 추출 + RAG 인덱싱
    file_extension = Path(file_name).suffix.lower()
    image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
    is_image = file_extension in image_extensions

    documents = []
    indexing_error: Optional[str] = None
    try:
        if is_image:
            from app.services.ingest.image import process_image_file
            file_id = storage_path.replace("/", "_").replace("\\", "_")
            documents = await process_image_file(
                file_content, file_name, file_id, tenant_id, None,
                storage_type="storage",
                storage_file_path=storage_path,
                public_url=public_url,
            )
            for doc in (documents or []):
                doc.metadata["knowledge_scope"] = "global"
        else:
            file_io = io.BytesIO(file_content)
            processor = get_document_processor()
            docs = await processor.load_document(file_io, file_name)
            if docs:
                documents = await processor.process_documents(docs, {
                    "storage_type": "storage",
                    "file_path": storage_path,
                    "file_name": file_name,
                    "tenant_id": tenant_id,
                })
                for doc in documents:
                    doc.metadata.update({
                        "file_id": storage_path,
                        "file_name": file_name,
                        "tenant_id": tenant_id,
                        "storage_type": "storage",
                        "knowledge_scope": "global",
                    })

        if documents:
            rag = get_rag_chain()
            success = await rag.process_and_store_documents(documents, tenant_id)
            if success:
                await rag.save_processed_files([storage_path], tenant_id, [file_name])
                await mark_status(
                    tenant_id=tenant_id,
                    source_type="upload",
                    source_ref=storage_path,
                    status=INDEX_STATUS_INDEXED,
                )
            else:
                indexing_error = "Vector store processing failed"
        else:
            indexing_error = "No content extracted"
    except Exception as e:
        indexing_error = str(e)
        logger.exception("[knowledge_admin] indexing failed for %s: %s", file_name, e)

    if indexing_error:
        await mark_status(
            tenant_id=tenant_id,
            source_type="upload",
            source_ref=storage_path,
            status=INDEX_STATUS_FAILED,
            error=indexing_error,
        )

    return {
        "source_type": "upload",
        "source_ref": storage_path,
        "file_name": file_name,
        "size_bytes": size_bytes,
        "public_url": public_url,
        "indexed": indexing_error is None,
        "error": indexing_error,
    }


@router.get("/knowledge/files/url")
async def get_knowledge_file_url(
    tenant_id: str = Query(...),
    source_type: str = Query(...),
    source_ref: str = Query(...),
    file_name: Optional[str] = Query(None),
):
    """파일 보기/다운로드용 URL 반환.
    - drive: Google Drive 웹 뷰어 URL (file_id 기반)
    - upload: Supabase Storage public/signed URL (file_name 지정 시 원본 파일명으로 다운로드)
    """
    from urllib.parse import quote

    from app.core.supabase_client import supabase

    if source_type == "drive":
        if not source_ref:
            raise HTTPException(status_code=400, detail="source_ref required")
        return {
            "url": f"https://drive.google.com/file/d/{source_ref}/view",
            "kind": "view",
        }

    if source_type == "upload":
        # file_name 미지정 시 knowledge_files에서 조회 — Content-Disposition 헤더로 원본 이름 다운로드
        if not file_name:
            try:
                import asyncio
                row = await asyncio.to_thread(
                    supabase.table("knowledge_files")
                    .select("file_name")
                    .eq("tenant_id", tenant_id)
                    .eq("source_type", "upload")
                    .eq("source_ref", source_ref)
                    .single()
                    .execute
                )
                file_name = (row.data or {}).get("file_name") or ""
            except Exception:
                file_name = ""

        try:
            url: Optional[str] = None
            # 1) signed URL 시도 (private 버킷 대응) — 1시간 만료
            try:
                signed = supabase.storage.from_("files").create_signed_url(source_ref, 3600)
                if isinstance(signed, dict):
                    url = signed.get("signedURL") or signed.get("signedUrl") or signed.get("url")
                else:
                    url = str(signed) if signed else None
            except Exception as e:
                logger.warning("[knowledge_admin] signed url failed, falling back to public: %s", e)

            # 2) public URL fallback
            if not url:
                public = supabase.storage.from_("files").get_public_url(source_ref)
                url = public.get("publicURL") if isinstance(public, dict) else str(public)

            # 3) 원본 파일명을 Content-Disposition으로 강제 — Supabase storage가 download 쿼리를 해석함
            if url and file_name:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}download={quote(file_name, safe='')}"

            return {"url": url, "kind": "download", "file_name": file_name}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to resolve URL: {e}")

    raise HTTPException(status_code=400, detail="invalid source_type")


@router.delete("/knowledge/files")
async def delete_knowledge_file(
    tenant_id: str = Query(...),
    source_type: str = Query(...),
    source_ref: str = Query(...),
    requester_uid: Optional[str] = Query(None),
    is_admin: bool = Query(False),
):
    """파일 1개를 RAG 인덱스 + storage + knowledge_files에서 완전 제거.

    권한:
      - 관리자(is_admin=True): 모든 파일 삭제 가능
      - 일반 사용자: 본인이 업로드한 'upload' 파일만 삭제 가능
      - drive 인덱스 항목: 관리자만 제거 가능
    """
    if source_type not in {"drive", "upload"}:
        raise HTTPException(status_code=400, detail="invalid source_type")

    if not is_admin:
        if source_type != "upload":
            raise HTTPException(status_code=403, detail="관리자만 이 항목을 제거할 수 있습니다.")
        entry = await get_entry(tenant_id=tenant_id, source_type=source_type, source_ref=source_ref)
        owner_uid = (entry or {}).get("uploaded_by_uid")
        if not requester_uid or not owner_uid or str(owner_uid) != str(requester_uid):
            raise HTTPException(status_code=403, detail="본인이 업로드한 파일만 삭제할 수 있습니다.")

    result = await delete_entry(
        tenant_id=tenant_id,
        source_type=source_type,
        source_ref=source_ref,
    )
    return {"ok": True, **result}


@router.get("/knowledge/folders")
async def list_knowledge_folders(tenant_id: str = Query(...)):
    """빈 폴더 포함 등록된 모든 폴더 경로 반환."""
    folders = await list_folders_for_tenant(tenant_id)
    return {"folders": folders}


@router.post("/knowledge/folders")
async def create_knowledge_folder(
    tenant_id: str = Form(...),
    folder_path: str = Form(...),
):
    """빈 폴더 생성 (knowledge_folders에 row 추가)."""
    folder_path = (folder_path or "").strip().strip("/")
    if not folder_path:
        raise HTTPException(status_code=400, detail="folder_path required")
    ok = await kf_create_folder(tenant_id=tenant_id, folder_path=folder_path)
    return {"ok": ok, "folder_path": folder_path}


@router.post("/knowledge/folders/rename")
async def rename_knowledge_folder(
    tenant_id: str = Form(...),
    old_path: str = Form(...),
    new_path: str = Form(...),
    is_admin: bool = Form(False),
):
    """업로드 폴더 이름 변경 — 하위 경로도 prefix 치환. (관리자 전용)"""
    if not is_admin:
        raise HTTPException(status_code=403, detail="관리자만 폴더를 변경할 수 있습니다.")
    old_path = (old_path or "").strip()
    new_path = (new_path or "").strip()
    if not old_path or not new_path:
        raise HTTPException(status_code=400, detail="old_path and new_path required")
    if old_path == new_path:
        return {"ok": True, "affected": 0}
    affected = await rename_folder(
        tenant_id=tenant_id,
        old_path=old_path,
        new_path=new_path,
    )
    return {"ok": True, "affected": affected}


@router.delete("/knowledge/folders")
async def delete_knowledge_folder(
    tenant_id: str = Query(...),
    folder_path: str = Query(...),
    is_admin: bool = Query(False),
):
    """업로드 폴더 삭제 — 하위 모든 파일을 storage/RAG/knowledge_files에서 영구 제거. (관리자 전용)"""
    if not is_admin:
        raise HTTPException(status_code=403, detail="관리자만 폴더를 삭제할 수 있습니다.")
    folder_path = (folder_path or "").strip()
    if not folder_path:
        raise HTTPException(status_code=400, detail="folder_path required")

    rows = await list_files_in_folder_recursive(
        tenant_id=tenant_id,
        folder_path=folder_path,
        source_type="upload",
    )
    deleted = 0
    failed = 0
    for r in rows:
        try:
            await delete_entry(
                tenant_id=tenant_id,
                source_type=r.get("source_type") or "upload",
                source_ref=r.get("source_ref") or "",
            )
            deleted += 1
        except Exception as e:
            logger.warning("delete folder file failed (%s): %s", r.get("source_ref"), e)
            failed += 1

    # knowledge_folders 메타 row도 정리 (빈 폴더 + 자식 폴더)
    await delete_folder_meta(tenant_id=tenant_id, folder_path=folder_path)

    return {"ok": True, "deleted": deleted, "failed": failed, "total": len(rows)}
