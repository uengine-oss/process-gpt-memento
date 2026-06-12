"""knowledge_files 테이블 헬퍼 — 내부 지식공간 파일 메타/상태 관리.

이 모듈은 Drive 인덱싱 + 추후 직접 업로드 모두에서 동일하게 사용된다.
RAG 청크(documents 테이블)와 분리해서 "파일 단위" 메타와 인덱싱 상태를 추적한다.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.supabase_client import supabase

logger = logging.getLogger(__name__)


def _sanitize_storage_segment(name: str) -> str:
    """Supabase storage key에 안전한 폴더 이름 변환.

    - ASCII + [a-zA-Z0-9._-]만 허용 → 그대로
    - 그 외 (한글 등): "f-{sha1[:10]}"로 안정적 해시
    """
    if not name:
        return ""
    if re.fullmatch(r"[a-zA-Z0-9._\-]+", name):
        return name
    # ASCII이지만 일부 특수문자 → 안전 치환
    if name.isascii():
        cleaned = re.sub(r"[^a-zA-Z0-9._\-]+", "-", name).strip("-")
        if cleaned:
            return cleaned
    # 비ASCII (한글 등) → 안정 해시
    h = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
    return f"f-{h}"


def sanitize_storage_folder_path(folder_path: str) -> str:
    """slash로 split된 각 segment를 sanitize."""
    if not folder_path:
        return ""
    return "/".join(_sanitize_storage_segment(p) for p in folder_path.split("/") if p)

INDEX_STATUS_PENDING = "pending"
INDEX_STATUS_PROCESSING = "processing"
INDEX_STATUS_INDEXED = "indexed"
INDEX_STATUS_FAILED = "failed"
INDEX_STATUS_EXCLUDED = "excluded"


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


async def upsert_drive_files(
    tenant_id: str,
    files: List[Dict[str, Any]],
) -> None:
    """Drive에서 나열된 파일들을 knowledge_files에 pending 상태로 upsert.

    Args:
        files: GoogleDriveLoader.list_files_recursive()가 돌려준 dict 배열
               (id, name, mimeType, size, modifiedTime, owners, drive_folder_id, drive_folder_name)
    """
    if not files:
        return

    rows: List[Dict[str, Any]] = []
    for f in files:
        if not isinstance(f, dict):
            continue
        file_id = f.get("id")
        file_name = f.get("name")
        if not file_id or not file_name:
            continue
        owners = f.get("owners") or []
        owner_str: Optional[str] = None
        if owners and isinstance(owners, list):
            first = owners[0] or {}
            owner_str = first.get("displayName") or first.get("emailAddress")
        rows.append({
            "tenant_id": tenant_id,
            "source_type": "drive",
            "source_ref": file_id,
            "file_name": file_name,
            "folder_path": f.get("drive_folder_name") or "",
            "drive_folder_id": f.get("drive_folder_id"),
            "mime_type": f.get("mimeType"),
            "size_bytes": _safe_int(f.get("size")),
            "modified_time": f.get("modifiedTime"),
            "owner": owner_str,
            # status는 신규 row에만 적용. 기존 row의 indexed 상태는 보존되어야 하므로
            # 아래 upsert에서 ignore_duplicates 대신 on_conflict로 부분 갱신한다.
        })

    if not rows:
        return

    try:
        # 메타 필드는 항상 최신으로 갱신하되, 상태 필드는 건드리지 않음.
        # supabase upsert는 전체 row를 덮어쓰므로 두 단계로 처리:
        #  1) 신규 row만 INSERT (on_conflict ignore)
        #  2) 기존 row의 메타 필드만 UPDATE
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .upsert(
                [{**r, "index_status": INDEX_STATUS_PENDING} for r in rows],
                on_conflict="tenant_id,source_type,source_ref",
                ignore_duplicates=True,
            )
            .execute
        )
        # 기존 row 메타 갱신
        for r in rows:
            await asyncio.to_thread(
                supabase.table("knowledge_files")
                .update({
                    "file_name": r["file_name"],
                    "folder_path": r["folder_path"],
                    "drive_folder_id": r["drive_folder_id"],
                    "mime_type": r["mime_type"],
                    "size_bytes": r["size_bytes"],
                    "modified_time": r["modified_time"],
                    "owner": r["owner"],
                })
                .eq("tenant_id", r["tenant_id"])
                .eq("source_type", r["source_type"])
                .eq("source_ref", r["source_ref"])
                .execute
            )
    except Exception as e:
        logger.warning("[knowledge_files] upsert_drive_files failed: %s", e)


async def mark_status(
    tenant_id: str,
    source_type: str,
    source_ref: str,
    status: str,
    error: Optional[str] = None,
) -> None:
    payload: Dict[str, Any] = {"index_status": status, "index_error": error}
    if status == INDEX_STATUS_INDEXED:
        payload["indexed_at"] = datetime.utcnow().isoformat()
        payload["index_error"] = None
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update(payload)
            .eq("tenant_id", tenant_id)
            .eq("source_type", source_type)
            .eq("source_ref", source_ref)
            .execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] mark_status failed (%s): %s", status, e)


VALID_DOC_ROLES = ("content", "glossary", "template", "reference", "dataset", "legal_review")


def normalize_doc_role(role: Optional[str]) -> str:
    """클라이언트가 보낸 doc_role 값 정규화 — 미지정/오타는 'content' 폴백."""
    r = (role or "").strip().lower()
    return r if r in VALID_DOC_ROLES else "content"


# 내부 호환용 alias (기존 코드가 underscore 버전 import 한 경우 대비)
_normalize_doc_role = normalize_doc_role


async def register_uploaded_file(
    tenant_id: str,
    storage_path: str,
    file_name: str,
    folder_path: Optional[str] = None,
    mime_type: Optional[str] = None,
    size_bytes: Optional[int] = None,
    owner: Optional[str] = None,
    initial_status: str = INDEX_STATUS_PROCESSING,
    file_hash: Optional[str] = None,
    uploaded_by_uid: Optional[str] = None,
    uploaded_by_name: Optional[str] = None,
    doc_role: Optional[str] = None,
) -> None:
    """직접 업로드한 파일을 knowledge_files에 등록한다 (source_type='upload')."""
    payload = {
        "tenant_id": tenant_id,
        "source_type": "upload",
        "source_ref": storage_path,
        "file_name": file_name,
        "folder_path": folder_path or "",
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "owner": owner,
        "uploaded_by_uid": uploaded_by_uid,
        "uploaded_by_name": uploaded_by_name,
        "file_hash": file_hash,
        "index_status": initial_status,
        "doc_role": _normalize_doc_role(doc_role),
        "modified_time": datetime.utcnow().isoformat(),
    }
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .upsert(payload, on_conflict="tenant_id,source_type,source_ref")
            .execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] register_uploaded_file failed: %s", e)


async def delete_entry(
    tenant_id: str,
    source_type: str,
    source_ref: str,
) -> Dict[str, Any]:
    """파일 1개를 RAG 인덱스/메타에서 *완전* 제거 — 더미 데이터 누수 0 목표.

    정리 대상 (모두):
      1) documents 테이블 청크 본문 (metadata.file_id 매칭)
      2) documents 테이블 이미지-분석 본문 (metadata.type='image_analysis' AND
         metadata.document_id ∈ 위 청크 id들)
      3) Chroma 컬렉션 임베딩 (청크 + 이미지-분석)
      4) document_pages 테이블 페이지 본문
      5) document_images 테이블 이미지 메타 (document_id ∈ 위 청크 id들)
      6) processed_files
      7) Storage 'files' 버킷의 원본 파일 (upload만)
      8) Storage extracted_images/<tenant>/<file_id>/ 폴더의 추출 이미지들
      9) knowledge_files row

    실패는 격리 — 한 단계 실패해도 나머지 단계 계속 진행. 각 단계 성공 여부는 result 에.
    """
    result: Dict[str, Any] = {
        "documents_deleted": False,
        "image_analysis_documents_deleted": False,
        "chroma_deleted": False,
        "pages_deleted": False,
        "document_images_deleted": False,
        "processed_files_deleted": False,
        "storage_deleted": False,
        "extracted_images_deleted": False,
        "knowledge_row_deleted": False,
    }

    # 0a. 청크 row id 미리 수집 — document_images / 이미지-분석 행 삭제 시 FK 역할
    chunk_ids: List[str] = []
    try:
        resp = await asyncio.to_thread(
            supabase.table("documents")
            .select("id")
            .eq("metadata->>tenant_id", tenant_id)
            .eq("metadata->>file_id", source_ref)
            .execute
        )
        chunk_ids = [str(r.get("id")) for r in (resp.data or []) if r.get("id")]
    except Exception as e:
        logger.warning("[knowledge_files] collect chunk ids failed: %s", e)

    # 0b. 이미지-분석 documents row id 수집 (metadata.document_id ∈ chunk_ids)
    #     이미지-분석 행은 자기 metadata.file_id 가 없어서 file_id 직접 매칭 불가 →
    #     parent 청크 id 통해 역추적.
    image_doc_ids: List[str] = []
    if chunk_ids:
        # PostgREST URL 길이 제한 대비 — 200개씩 배치
        for start in range(0, len(chunk_ids), 200):
            batch = chunk_ids[start:start + 200]
            try:
                resp = await asyncio.to_thread(
                    supabase.table("documents")
                    .select("id")
                    .eq("metadata->>type", "image_analysis")
                    .in_("metadata->>document_id", batch)
                    .execute
                )
                image_doc_ids.extend(
                    str(r.get("id")) for r in (resp.data or []) if r.get("id")
                )
            except Exception as e:
                logger.warning(
                    "[knowledge_files] collect image-analysis ids (batch %d) failed: %s",
                    start // 200, e,
                )

    # 1. Chroma 임베딩 삭제 (documents row 삭제 *전*에 — 의존성은 없지만 보수적 순서)
    try:
        from app.services.vector_store import get_vector_store
        vsm = get_vector_store()
        # 청크 임베딩 — Chroma metadata 의 tenant_id + file_id 로 매칭
        await asyncio.to_thread(
            vsm.collection.delete,
            where={"$and": [
                {"tenant_id": tenant_id},
                {"file_id": source_ref},
            ]},
        )
        # 이미지-분석 임베딩 — file_id 가 없어서 row id 로 직접 삭제
        if image_doc_ids:
            for start in range(0, len(image_doc_ids), 500):
                batch = image_doc_ids[start:start + 500]
                await asyncio.to_thread(vsm.collection.delete, ids=batch)
        result["chroma_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] delete Chroma embeddings failed: %s", e)

    # 2. document_images 메타 삭제 (chunk_ids 기반)
    if chunk_ids:
        try:
            for start in range(0, len(chunk_ids), 200):
                batch = chunk_ids[start:start + 200]
                await asyncio.to_thread(
                    supabase.table("document_images")
                    .delete()
                    .in_("document_id", batch)
                    .execute
                )
            result["document_images_deleted"] = True
        except Exception as e:
            logger.warning("[knowledge_files] delete document_images failed: %s", e)
    else:
        result["document_images_deleted"] = True  # 청크 없으면 정리할 것도 없음

    # 3. documents 청크 본문 삭제
    try:
        await asyncio.to_thread(
            supabase.table("documents")
            .delete()
            .eq("metadata->>tenant_id", tenant_id)
            .eq("metadata->>file_id", source_ref)
            .execute
        )
        result["documents_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] delete documents (chunks) failed: %s", e)

    # 3b. documents 이미지-분석 본문 삭제
    if image_doc_ids:
        try:
            for start in range(0, len(image_doc_ids), 200):
                batch = image_doc_ids[start:start + 200]
                await asyncio.to_thread(
                    supabase.table("documents")
                    .delete()
                    .in_("id", batch)
                    .execute
                )
            result["image_analysis_documents_deleted"] = True
        except Exception as e:
            logger.warning(
                "[knowledge_files] delete image-analysis documents failed: %s", e,
            )
    else:
        result["image_analysis_documents_deleted"] = True

    # 4. document_pages 삭제 (Phase 1.1 신규 테이블)
    try:
        await asyncio.to_thread(
            supabase.table("document_pages")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("file_id", source_ref)
            .execute
        )
        result["pages_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] delete document_pages failed: %s", e)

    # 5. processed_files 삭제 (재인덱싱 가능하도록)
    try:
        await asyncio.to_thread(
            supabase.table("processed_files")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("file_id", source_ref)
            .execute
        )
        result["processed_files_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] delete processed_files failed: %s", e)

    # 6. Storage 'files' 버킷의 원본 (upload 만)
    if source_type == "upload":
        try:
            await asyncio.to_thread(
                supabase.storage.from_("files").remove, [source_ref]
            )
            result["storage_deleted"] = True
        except Exception as e:
            logger.warning("[knowledge_files] delete storage object failed: %s", e)
    else:
        result["storage_deleted"] = True  # drive 등 외부 소스는 우리 storage 객체 없음

    # 7. Storage extracted_images/<tenant>/<file_id>/ 폴더 정리
    #    file_id 가 storage path(슬래시 포함)면 깊은 nested 폴더가 됨 — 그대로 처리.
    try:
        folder = f"extracted_images/{tenant_id}/{source_ref}"
        objects = await asyncio.to_thread(
            supabase.storage.from_("files").list, folder
        )
        if objects:
            paths = [
                f"{folder}/{obj['name']}"
                for obj in objects
                if isinstance(obj, dict) and obj.get("name")
            ]
            if paths:
                # remove 는 한 호출에 여러 path OK — 다만 너무 많으면 분할
                for start in range(0, len(paths), 100):
                    await asyncio.to_thread(
                        supabase.storage.from_("files").remove,
                        paths[start:start + 100],
                    )
        result["extracted_images_deleted"] = True
    except Exception as e:
        logger.warning(
            "[knowledge_files] delete extracted_images folder failed: %s", e,
        )

    # 8. knowledge_files row 삭제 — 마지막 (위 단계들이 source_ref 매칭에 의존)
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("source_type", source_type)
            .eq("source_ref", source_ref)
            .execute
        )
        result["knowledge_row_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] delete knowledge_files row failed: %s", e)

    logger.info(
        "[knowledge_files] delete_entry result tenant=%s ref=%s : %s",
        tenant_id, source_ref, result,
    )
    return result


async def _update_documents_file_id(
    tenant_id: str, old_file_id: str, new_file_id: str
) -> None:
    """documents 테이블의 metadata.file_id, metadata.file_path를 갱신 (RAG 청크 N개)."""
    try:
        result = await asyncio.to_thread(
            supabase.table("documents")
            .select("id, metadata")
            .eq("metadata->>tenant_id", tenant_id)
            .eq("metadata->>file_id", old_file_id)
            .execute
        )
        for row in (result.data or []):
            md = row.get("metadata") or {}
            if md.get("file_id") == old_file_id:
                md["file_id"] = new_file_id
            if md.get("file_path") == old_file_id:
                md["file_path"] = new_file_id
            try:
                await asyncio.to_thread(
                    supabase.table("documents")
                    .update({"metadata": md})
                    .eq("id", row["id"])
                    .execute
                )
            except Exception as e:
                logger.warning("[knowledge_files] update document chunk %s failed: %s", row.get("id"), e)
    except Exception as e:
        logger.warning("[knowledge_files] update documents failed: %s", e)


async def _update_processed_file_id(
    tenant_id: str, old_file_id: str, new_file_id: str
) -> None:
    try:
        await asyncio.to_thread(
            supabase.table("processed_files")
            .update({"file_id": new_file_id})
            .eq("tenant_id", tenant_id)
            .eq("file_id", old_file_id)
            .execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] update processed_files failed: %s", e)


async def _move_storage_object(old_path: str, new_path: str) -> bool:
    """Supabase Storage 'files' 버킷에서 객체를 이동. supabase-py의 move() 사용."""
    if old_path == new_path:
        return True
    try:
        await asyncio.to_thread(
            supabase.storage.from_("files").move, old_path, new_path
        )
        return True
    except Exception as e:
        # move 미지원/실패 시 copy + remove로 fallback
        logger.warning("[knowledge_files] storage move failed (%s -> %s): %s; trying copy+remove", old_path, new_path, e)
        try:
            await asyncio.to_thread(
                supabase.storage.from_("files").copy, old_path, new_path
            )
            await asyncio.to_thread(
                supabase.storage.from_("files").remove, [old_path]
            )
            return True
        except Exception as e2:
            logger.error("[knowledge_files] storage copy+remove failed: %s", e2)
            return False


async def _move_one_file(
    tenant_id: str,
    row: Dict[str, Any],
    new_folder_path: str,
) -> bool:
    """단일 파일을 새 folder_path로 이동:
    1) storage move (source_ref → new_storage_path)
    2) knowledge_files update (source_ref + folder_path)
    3) documents.metadata.file_id 갱신
    4) processed_files.file_id 갱신
    """
    old_ref = row.get("source_ref") or ""
    if not old_ref:
        return False

    # storage path 구조: knowledge/{tenant}/{sanitize(folder_path)}/{uuid}.ext
    # source_ref의 마지막 segment(= uuid 파일명)만 떼서 새 folder 아래에 붙임
    file_basename = old_ref.rsplit("/", 1)[-1]
    base_prefix = f"knowledge/{tenant_id}"
    safe_new_folder = sanitize_storage_folder_path(new_folder_path)
    new_ref = (
        f"{base_prefix}/{safe_new_folder}/{file_basename}"
        if safe_new_folder
        else f"{base_prefix}/{file_basename}"
    )

    if old_ref == new_ref:
        # 경로 변화 없음 — DB만 갱신
        try:
            await asyncio.to_thread(
                supabase.table("knowledge_files")
                .update({"folder_path": new_folder_path})
                .eq("tenant_id", tenant_id)
                .eq("source_type", "upload")
                .eq("source_ref", old_ref)
                .execute
            )
            return True
        except Exception as e:
            logger.warning("[knowledge_files] update folder_path failed: %s", e)
            return False

    moved = await _move_storage_object(old_ref, new_ref)
    if not moved:
        return False

    try:
        await asyncio.to_thread(
            supabase.table("knowledge_files")
            .update({"folder_path": new_folder_path, "source_ref": new_ref})
            .eq("tenant_id", tenant_id)
            .eq("source_type", "upload")
            .eq("source_ref", old_ref)
            .execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] update knowledge_files row failed: %s", e)
        return False

    await _update_documents_file_id(tenant_id, old_ref, new_ref)
    await _update_processed_file_id(tenant_id, old_ref, new_ref)
    return True


async def rename_folder(
    tenant_id: str,
    old_path: str,
    new_path: str,
    doc_role: Optional[str] = None,
) -> int:
    """upload 소스의 폴더 이름을 변경. doc_role 지정 시 해당 role 안에서만.
    - knowledge_files.folder_path prefix 치환 (role scope)
    - storage 객체도 새 경로로 move
    - documents.metadata.file_id, processed_files.file_id 도 동기화

    Returns: 성공한 row 수
    """
    if not old_path or not new_path or old_path == new_path:
        return 0

    role = _normalize_doc_role(doc_role) if doc_role else None
    affected = 0

    # 1) 정확히 그 폴더의 파일들
    try:
        eq = (
            supabase.table("knowledge_files")
            .select("source_ref, folder_path")
            .eq("tenant_id", tenant_id)
            .eq("source_type", "upload")
            .eq("folder_path", old_path)
        )
        if role:
            eq = eq.eq("doc_role", role)
        exact = await asyncio.to_thread(eq.execute)
        for row in (exact.data or []):
            ok = await _move_one_file(tenant_id, row, new_path)
            if ok:
                affected += 1
    except Exception as e:
        logger.warning("[knowledge_files] rename exact query failed: %s", e)

    # 2) 하위 폴더 파일들 — folder_path가 old_path/로 시작
    try:
        cq = (
            supabase.table("knowledge_files")
            .select("source_ref, folder_path")
            .eq("tenant_id", tenant_id)
            .eq("source_type", "upload")
            .like("folder_path", f"{old_path}/%")
        )
        if role:
            cq = cq.eq("doc_role", role)
        children = await asyncio.to_thread(cq.execute)
        for row in (children.data or []):
            old_folder = row.get("folder_path") or ""
            new_folder = new_path + old_folder[len(old_path):]
            ok = await _move_one_file(tenant_id, row, new_folder)
            if ok:
                affected += 1
    except Exception as e:
        logger.warning("[knowledge_files] rename children query failed: %s", e)

    # 3) knowledge_folders 메타 row도 같이 갱신 (빈 폴더 영속화)
    await rename_folder_meta(tenant_id, old_path, new_path, doc_role=role)

    return affected


async def list_files_in_folder_recursive(
    tenant_id: str,
    folder_path: str,
    source_type: str = "upload",
    doc_role: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """특정 폴더 + 그 하위에 속한 파일 row 반환. doc_role 지정 시 해당 role 안에서만."""
    rows: List[Dict[str, Any]] = []
    role = _normalize_doc_role(doc_role) if doc_role else None
    try:
        eq = (
            supabase.table("knowledge_files")
            .select("source_type, source_ref, file_name, folder_path")
            .eq("tenant_id", tenant_id)
            .eq("source_type", source_type)
            .eq("folder_path", folder_path)
        )
        if role:
            eq = eq.eq("doc_role", role)
        exact = await asyncio.to_thread(eq.execute)
        rows.extend(exact.data or [])
    except Exception as e:
        logger.warning("[knowledge_files] folder list exact failed: %s", e)

    try:
        cq = (
            supabase.table("knowledge_files")
            .select("source_type, source_ref, file_name, folder_path")
            .eq("tenant_id", tenant_id)
            .eq("source_type", source_type)
            .like("folder_path", f"{folder_path}/%")
        )
        if role:
            cq = cq.eq("doc_role", role)
        children = await asyncio.to_thread(cq.execute)
        rows.extend(children.data or [])
    except Exception as e:
        logger.warning("[knowledge_files] folder list children failed: %s", e)

    return rows


async def list_folders_for_tenant(tenant_id: str) -> List[Dict[str, Any]]:
    """knowledge_folders 테이블에서 빈 폴더 포함 모든 등록된 폴더 row 반환.

    Returns:
        [{"folder_path": str, "doc_role": str}, ...]
    """
    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_folders")
            .select("folder_path, doc_role")
            .eq("tenant_id", tenant_id)
            .execute
        )
        return [
            {
                "folder_path": r["folder_path"],
                "doc_role": (r.get("doc_role") or "content"),
            }
            for r in (result.data or [])
            if r.get("folder_path")
        ]
    except Exception as e:
        logger.warning("[knowledge_folders] list failed: %s", e)
        return []


async def create_folder(tenant_id: str, folder_path: str, doc_role: Optional[str] = None) -> bool:
    folder_path = (folder_path or "").strip().strip("/")
    if not folder_path:
        return False
    role = _normalize_doc_role(doc_role)
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_folders")
            .upsert(
                {"tenant_id": tenant_id, "folder_path": folder_path, "doc_role": role},
                on_conflict="tenant_id,doc_role,folder_path",
            )
            .execute
        )
        return True
    except Exception as e:
        logger.warning("[knowledge_folders] create failed: %s", e)
        return False


async def rename_folder_meta(
    tenant_id: str,
    old_path: str,
    new_path: str,
    doc_role: Optional[str] = None,
) -> int:
    """knowledge_folders 테이블에서 폴더 row 자체와 자식 폴더들 prefix 치환.
    rename_folder()에서 함께 호출됨. doc_role 지정 시 해당 role 안에서만 적용.
    """
    if not old_path or not new_path or old_path == new_path:
        return 0
    role = _normalize_doc_role(doc_role) if doc_role else None
    affected = 0
    try:
        q = (
            supabase.table("knowledge_folders")
            .update({"folder_path": new_path})
            .eq("tenant_id", tenant_id)
            .eq("folder_path", old_path)
        )
        if role:
            q = q.eq("doc_role", role)
        await asyncio.to_thread(q.execute)
        affected += 1
    except Exception as e:
        logger.warning("[knowledge_folders] rename exact failed: %s", e)

    try:
        cq = (
            supabase.table("knowledge_folders")
            .select("id, folder_path")
            .eq("tenant_id", tenant_id)
            .like("folder_path", f"{old_path}/%")
        )
        if role:
            cq = cq.eq("doc_role", role)
        children = await asyncio.to_thread(cq.execute)
        for row in (children.data or []):
            old_p = row.get("folder_path") or ""
            new_p = new_path + old_p[len(old_path):]
            try:
                await asyncio.to_thread(
                    supabase.table("knowledge_folders")
                    .update({"folder_path": new_p})
                    .eq("id", row["id"])
                    .execute
                )
                affected += 1
            except Exception as e:
                logger.warning("[knowledge_folders] rename child failed: %s", e)
    except Exception as e:
        logger.warning("[knowledge_folders] rename children query failed: %s", e)

    return affected


async def delete_folder_meta(
    tenant_id: str,
    folder_path: str,
    doc_role: Optional[str] = None,
) -> int:
    """knowledge_folders에서 해당 폴더 + 모든 자식 폴더 row 삭제.
    doc_role 지정 시 해당 role 안에서만 삭제.
    """
    if not folder_path:
        return 0
    role = _normalize_doc_role(doc_role) if doc_role else None
    try:
        q1 = (
            supabase.table("knowledge_folders")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("folder_path", folder_path)
        )
        if role:
            q1 = q1.eq("doc_role", role)
        await asyncio.to_thread(q1.execute)

        q2 = (
            supabase.table("knowledge_folders")
            .delete()
            .eq("tenant_id", tenant_id)
            .like("folder_path", f"{folder_path}/%")
        )
        if role:
            q2 = q2.eq("doc_role", role)
        await asyncio.to_thread(q2.execute)
        return 1
    except Exception as e:
        logger.warning("[knowledge_folders] delete failed: %s", e)
        return 0


async def find_by_hash(tenant_id: str, file_hash: str) -> Optional[Dict[str, Any]]:
    """동일 테넌트 내에서 같은 SHA-256 해시를 가진 첫 번째 파일 row 반환.

    중복 업로드 감지용. upload + drive 양쪽 다 검사한다.
    """
    if not tenant_id or not file_hash:
        return None
    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select(
                "source_type, source_ref, file_name, folder_path, "
                "mime_type, size_bytes, modified_time, indexed_at, index_status"
            )
            .eq("tenant_id", tenant_id)
            .eq("file_hash", file_hash)
            .limit(1)
            .execute
        )
        rows = result.data or []
        return rows[0] if rows else None
    except Exception as e:
        logger.warning("[knowledge_files] find_by_hash failed: %s", e)
        return None


async def get_entry(
    tenant_id: str,
    source_type: str,
    source_ref: str,
) -> Optional[Dict[str, Any]]:
    """knowledge_files에서 단일 파일 row 조회 (권한 체크 등에 사용)."""
    if not tenant_id or not source_ref:
        return None
    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select(
                "source_type, source_ref, file_name, folder_path, owner, "
                "uploaded_by_uid, uploaded_by_name"
            )
            .eq("tenant_id", tenant_id)
            .eq("source_type", source_type)
            .eq("source_ref", source_ref)
            .limit(1)
            .execute
        )
        rows = result.data or []
        return rows[0] if rows else None
    except Exception as e:
        logger.warning("[knowledge_files] get_entry failed: %s", e)
        return None


async def list_by_role(
    tenant_id: str,
    doc_role: str,
    source_refs: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """tenant 안에서 doc_role 매칭 파일 row 반환 (옵션: source_refs로 추가 필터)."""
    if not tenant_id or not doc_role:
        return []
    try:
        q = (
            supabase.table("knowledge_files")
            .select("source_type, source_ref, file_name, folder_path, doc_role")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", doc_role)
        )
        cleaned = [s for s in (source_refs or []) if s]
        if cleaned:
            q = q.in_("source_ref", cleaned)
        result = await asyncio.to_thread(q.execute)
        return list(result.data or [])
    except Exception as e:
        logger.warning("[knowledge_files] list_by_role(%s) failed: %s", doc_role, e)
        return []


async def list_for_tenant(tenant_id: str) -> List[Dict[str, Any]]:
    """프론트 picker용 — 테넌트의 모든 knowledge_files row 반환."""
    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select(
                "source_type, source_ref, file_name, folder_path, drive_folder_id, "
                "mime_type, size_bytes, modified_time, owner, "
                "uploaded_by_uid, uploaded_by_name, index_status, "
                "index_error, indexed_at, updated_at, doc_role"
            )
            .eq("tenant_id", tenant_id)
            .order("folder_path", desc=False)
            .order("file_name", desc=False)
            .execute
        )
        return list(result.data or [])
    except Exception as e:
        logger.warning("[knowledge_files] list_for_tenant failed: %s", e)
        return []
