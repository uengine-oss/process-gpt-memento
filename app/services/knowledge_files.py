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
from urllib.parse import quote

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


def compose_path(folder_path: Optional[str], file_name: Optional[str]) -> str:
    """folder_path + file_name → 전체 상대경로(에이전트가 다루는 *단일 핸들*).

    folder_path 비면 file_name 만. 앞/끝 슬래시 정리. file_name 은 basename 가정.
    예: ("mock-corpus/A/05", "Credit Agreement.pdf") → "mock-corpus/A/05/Credit Agreement.pdf"
    """
    fp = (folder_path or "").strip().strip("/")
    fn = (file_name or "").strip()
    return f"{fp}/{fn}" if fp else fn

INDEX_STATUS_PENDING = "pending"
INDEX_STATUS_PROCESSING = "processing"
INDEX_STATUS_INDEXED = "indexed"
INDEX_STATUS_FAILED = "failed"
INDEX_STATUS_EXCLUDED = "excluded"

# 채팅 첨부(임시/세션 업로드)의 source_ref prefix. 이 파일들은 knowledge_files 에 등록돼
# 에이전트가 file_id 로 읽을 수 있지만, KB 브라우저/폴더트리/카탈로그 *전체조회* 에는 안 뜬다.
#  · files/…   : /save-to-storage (채팅 첨부 — 프론트 업로드)
#  · session/… : /process-session-file (에이전트 폴백 ingest)
_CHAT_ATTACHMENT_PREFIXES = ("files/", "session/")


def _is_chat_attachment_ref(source_ref) -> bool:
    """source_ref 가 채팅 첨부(전체조회에서 숨길 대상)인가."""
    return str(source_ref or "").startswith(_CHAT_ATTACHMENT_PREFIXES)


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
            "path": compose_path(f.get("drive_folder_name"), file_name),
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
                    "path": r["path"],
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
    # updated_at 을 명시적으로 갱신 — 백그라운드 인제스트의 좀비(오래된 processing) 판정이
    # 이 타임스탬프에 의존하므로, DB 트리거 유무와 무관하게 항상 최신화한다.
    payload["updated_at"] = datetime.utcnow().isoformat()
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


# ── 업로드 허용 확장자 정책 (doc_role 별) ──────────────────────────────────
# 지식베이스 업로드는 분류(doc_role)별로 받는 확장자를 제한한다.
#   content/reference          : 일반 문서 (pdf/hwp/hwpx/doc/docx/pptx/txt)
#   glossary (용어 사전)       : 고정형 CSV(영문,한글뜻,약어) 전용 — glossary_terms 테이블로
#                                직행시키는 소스 (term-lock 소비). csv 만 허용.
#   template (양식)            : 편집형 양식만 (hwpx/docx)
#   dataset (데이터)           : 정량 데이터만 (xlsx)
#   legal_review (검토 사례)   : 변호사 메모 추출이 docx XML 한정 → docx 만
_DOC_EXTS: tuple = (".pdf", ".hwp", ".hwpx", ".doc", ".docx", ".pptx", ".txt")
ROLE_ALLOWED_EXTENSIONS: Dict[str, tuple] = {
    "content": _DOC_EXTS,
    "glossary": (".csv",),
    "reference": _DOC_EXTS,
    "template": (".hwpx", ".docx"),
    "dataset": (".xlsx",),
    "legal_review": (".docx",),
}


def allowed_extensions_for_role(role: Optional[str]) -> tuple:
    """해당 doc_role 에서 업로드 허용되는 확장자 튜플."""
    return ROLE_ALLOWED_EXTENSIONS.get(normalize_doc_role(role), _DOC_EXTS)


def is_extension_allowed_for_role(file_name: str, role: Optional[str]) -> bool:
    """file_name 의 확장자가 해당 doc_role 에서 허용되는지."""
    name = file_name or ""
    ext = ("." + name.rsplit(".", 1)[-1].lower()) if "." in name else ""
    return ext in allowed_extensions_for_role(role)


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
        "path": compose_path(folder_path, file_name),
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
    #    쓰기 락 공유(delete_where/ids)로 임베딩과 겹쳐도 단일 writer 충돌 없이 직렬화.
    try:
        from app.services.vector_store import get_vector_store
        vsm = get_vector_store()
        # 청크 임베딩 — Chroma metadata 의 tenant_id + file_id 로 매칭
        await vsm.delete_where(
            {"$and": [
                {"tenant_id": tenant_id},
                {"file_id": source_ref},
            ]}
        )
        # 이미지-분석 임베딩 — file_id 가 없어서 row id 로 직접 삭제
        if image_doc_ids:
            for start in range(0, len(image_doc_ids), 500):
                batch = image_doc_ids[start:start + 500]
                await vsm.delete_ids(batch)
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


def _chunked(seq: List[Any], size: int) -> List[List[Any]]:
    """리스트를 size 단위 배치로 분할."""
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def _chunk_by_urllen(
    seq: List[Any], max_encoded: int = 5000, max_count: int = 200
) -> List[List[Any]]:
    """PostgREST ``in.(...)`` 필터의 *URL 길이* 안전 배치.

    source_ref/file_id 는 한글 스토리지 경로라 URL 인코딩하면 한 건이 수백 자로 부풀어,
    고정 개수(예: 200)로 묶으면 파일 100여 개부터 URL 이 kong/PostgREST 한계(~8KB)를 넘겨
    요청이 *통째로 실패* → 삭제가 조용히 누락된다(성공으로 오인). 각 배치의 인코딩 길이 합을
    예산 이하로 유지해 이 실패를 원천 차단한다. (개수 상한도 병행 — 짧은 UUID 다량 대비.)
    """
    batches: List[List[Any]] = []
    cur: List[Any] = []
    cur_len = 0
    for it in seq:
        enc = len(quote(str(it), safe="")) + 3  # 구분자/따옴표 여유
        if cur and (cur_len + enc > max_encoded or len(cur) >= max_count):
            batches.append(cur)
            cur, cur_len = [], 0
        cur.append(it)
        cur_len += enc
    if cur:
        batches.append(cur)
    return batches


async def clear_index_artifacts(tenant_id: str, source_ref: str) -> None:
    """한 파일의 RAG 인덱스 산출물만 정리 — *스토리지 원본/knowledge_files row 는 유지*.

    재인덱싱(재시도·재시작 복구·reindex) 을 멱등하게 만들기 위해 *인덱싱 시작 전*에 호출한다.
    정리 대상: documents(청크 + 이미지-분석), document_images, document_pages,
    processed_files, Chroma 임베딩. (delete_entry 의 부분집합 — storage/row 는 안 건드림)
    최초 인덱싱(이전 산출물 없음)에는 사실상 no-op(인덱스 조회라 저렴).
    """
    if not tenant_id or not source_ref:
        return

    # 0a. 청크 id 수집 (이미지-분석/이미지메타 삭제의 FK)
    chunk_ids: List[str] = []
    try:
        resp = await asyncio.to_thread(
            supabase.table("documents").select("id")
            .eq("metadata->>tenant_id", tenant_id)
            .eq("metadata->>file_id", source_ref)
            .execute
        )
        chunk_ids = [str(r.get("id")) for r in (resp.data or []) if r.get("id")]
    except Exception as e:
        logger.warning("[knowledge_files] clear: collect chunk ids failed: %s", e)

    # 0b. 이미지-분석 documents id (document_id ∈ chunk_ids)
    image_doc_ids: List[str] = []
    for batch in _chunk_by_urllen(chunk_ids):
        try:
            resp = await asyncio.to_thread(
                supabase.table("documents").select("id")
                .eq("metadata->>type", "image_analysis")
                .in_("metadata->>document_id", batch)
                .execute
            )
            image_doc_ids.extend(str(r.get("id")) for r in (resp.data or []) if r.get("id"))
        except Exception as e:
            logger.warning("[knowledge_files] clear: collect image ids failed: %s", e)

    # 1. Chroma 임베딩 (청크 file_id + 이미지-분석 id) — 쓰기 락 공유(delete_where/ids)로
    #    재인덱싱·삭제가 임베딩과 겹쳐도 단일 writer 충돌 없이 직렬화된다.
    try:
        from app.services.vector_store import get_vector_store
        vsm = get_vector_store()
        await vsm.delete_where(
            {"$and": [{"tenant_id": tenant_id}, {"file_id": source_ref}]}
        )
        for batch in _chunked(image_doc_ids, 500):
            await vsm.delete_ids(batch)
    except Exception as e:
        logger.warning("[knowledge_files] clear: chroma delete failed: %s", e)

    # 2. document_images
    for batch in _chunk_by_urllen(chunk_ids):
        try:
            await asyncio.to_thread(
                supabase.table("document_images").delete().in_("document_id", batch).execute
            )
        except Exception as e:
            logger.warning("[knowledge_files] clear: document_images failed: %s", e)

    # 3. documents 청크 본문 (file_id)
    try:
        await asyncio.to_thread(
            supabase.table("documents").delete()
            .eq("metadata->>tenant_id", tenant_id)
            .eq("metadata->>file_id", source_ref)
            .execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] clear: documents(chunks) failed: %s", e)

    # 3b. documents 이미지-분석 본문 (id)
    for batch in _chunk_by_urllen(image_doc_ids):
        try:
            await asyncio.to_thread(
                supabase.table("documents").delete().in_("id", batch).execute
            )
        except Exception as e:
            logger.warning("[knowledge_files] clear: documents(image) failed: %s", e)

    # 4. document_pages (file_id 실제 컬럼)
    try:
        await asyncio.to_thread(
            supabase.table("document_pages").delete()
            .eq("tenant_id", tenant_id).eq("file_id", source_ref).execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] clear: document_pages failed: %s", e)

    # 5. processed_files (file_id 실제 컬럼) — 재인덱싱 가능하도록
    try:
        await asyncio.to_thread(
            supabase.table("processed_files").delete()
            .eq("tenant_id", tenant_id).eq("file_id", source_ref).execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] clear: processed_files failed: %s", e)


async def delete_entries_bulk(
    tenant_id: str,
    entries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """여러 파일을 *집합(set) 기반*으로 한 번에 삭제 — 폴더 삭제용 고속 경로.

    delete_entry 를 파일마다 호출하면 파일당 4~5회의 풀스캔/round-trip 이 N번 쌓여
    폴더 삭제가 폭발한다. 이 함수는 동일한 정리 대상(1~9단계)을 file_id/청크 id 리스트에
    대한 IN 절 배치로 묶어 호출 수를 자릿수 단위로 줄인다.
    (성능은 sql/perf_knowledge_indexes.sql 의 인덱스가 깔려 있어야 제대로 난다.)

    Args:
        entries: [{"source_type": ..., "source_ref": ...}, ...]
    Returns:
        {"total", "chunk_count", "image_doc_count", 단계별 ok 플래그}
    """
    # source_ref 중복 제거 + upload 분리(스토리지 원본은 upload 만 존재)
    refs: List[str] = []
    seen: set[str] = set()
    upload_refs: List[str] = []
    for e in entries:
        ref = (e.get("source_ref") or "").strip()
        if not ref or ref in seen:
            continue
        seen.add(ref)
        refs.append(ref)
        if (e.get("source_type") or "upload") == "upload":
            upload_refs.append(ref)

    result: Dict[str, Any] = {
        "total": len(refs),
        "chunk_count": 0,
        "image_doc_count": 0,
        "documents_deleted": False,
        "image_analysis_documents_deleted": False,
        "chroma_deleted": False,
        "document_images_deleted": False,
        "pages_deleted": False,
        "processed_files_deleted": False,
        "storage_deleted": False,
        "knowledge_rows_deleted": False,
    }
    if not refs:
        return result

    _REF_BATCH = 200   # PostgREST URL 길이 대비 IN 절 배치
    _ID_BATCH = 200
    _CHROMA_BATCH = 256

    # 0a. 청크 id 수집 (file_id IN refs) — image-분석/이미지메타 삭제의 FK
    chunk_ids: List[str] = []
    for batch in _chunk_by_urllen(refs):
        try:
            resp = await asyncio.to_thread(
                supabase.table("documents")
                .select("id")
                .eq("metadata->>tenant_id", tenant_id)
                .in_("metadata->>file_id", batch)
                .execute
            )
            chunk_ids.extend(str(r.get("id")) for r in (resp.data or []) if r.get("id"))
        except Exception as e:
            logger.warning("[knowledge_files] bulk collect chunk ids failed: %s", e)
    result["chunk_count"] = len(chunk_ids)

    # 0b. 이미지-분석 documents id 수집 (document_id ∈ chunk_ids)
    image_doc_ids: List[str] = []
    for batch in _chunk_by_urllen(chunk_ids):
        try:
            resp = await asyncio.to_thread(
                supabase.table("documents")
                .select("id")
                .eq("metadata->>type", "image_analysis")
                .in_("metadata->>document_id", batch)
                .execute
            )
            image_doc_ids.extend(str(r.get("id")) for r in (resp.data or []) if r.get("id"))
        except Exception as e:
            logger.warning("[knowledge_files] bulk collect image-analysis ids failed: %s", e)
    result["image_doc_count"] = len(image_doc_ids)

    # 1. Chroma 임베딩 삭제 (file_id $in 배치 + 이미지-분석은 id 로)
    #    *쓰기 락 공유* — 임베딩(add_documents)과 같은 인덱스 쓰기 락 아래에서 돌아
    #    동시 인제스트 중에도 SQLite/HNSW 충돌·스래싱 없이 협조 직렬화된다.
    try:
        from app.services.vector_store import get_vector_store
        vsm = get_vector_store()
        for batch in _chunked(refs, _CHROMA_BATCH):
            await vsm.delete_where(
                {"$and": [
                    {"tenant_id": tenant_id},
                    {"file_id": {"$in": batch}},
                ]}
            )
        for batch in _chunked(image_doc_ids, 500):
            await vsm.delete_ids(batch)
        result["chroma_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete Chroma embeddings failed: %s", e)

    # 2. document_images 메타 삭제 (document_id ∈ chunk_ids)
    try:
        for batch in _chunk_by_urllen(chunk_ids):
            await asyncio.to_thread(
                supabase.table("document_images").delete().in_("document_id", batch).execute
            )
        result["document_images_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete document_images failed: %s", e)

    # 3. documents 청크 본문 삭제 (file_id IN refs)
    try:
        for batch in _chunk_by_urllen(refs):
            await asyncio.to_thread(
                supabase.table("documents")
                .delete()
                .eq("metadata->>tenant_id", tenant_id)
                .in_("metadata->>file_id", batch)
                .execute
            )
        result["documents_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete documents (chunks) failed: %s", e)

    # 3b. documents 이미지-분석 본문 삭제 (id 로)
    try:
        for batch in _chunk_by_urllen(image_doc_ids):
            await asyncio.to_thread(
                supabase.table("documents").delete().in_("id", batch).execute
            )
        result["image_analysis_documents_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete image-analysis documents failed: %s", e)

    # 4. document_pages 삭제 (file_id IN refs — 실제 컬럼)
    try:
        for batch in _chunk_by_urllen(refs):
            await asyncio.to_thread(
                supabase.table("document_pages")
                .delete()
                .eq("tenant_id", tenant_id)
                .in_("file_id", batch)
                .execute
            )
        result["pages_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete document_pages failed: %s", e)

    # 5. processed_files 삭제 (file_id IN refs — 실제 컬럼)
    try:
        for batch in _chunk_by_urllen(refs):
            await asyncio.to_thread(
                supabase.table("processed_files")
                .delete()
                .eq("tenant_id", tenant_id)
                .in_("file_id", batch)
                .execute
            )
        result["processed_files_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete processed_files failed: %s", e)

    # 6. Storage — 'files' 버킷 원본(upload) + extracted_images 폴더
    try:
        for batch in _chunked(upload_refs, 100):
            if batch:
                await asyncio.to_thread(supabase.storage.from_("files").remove, batch)
        for ref in upload_refs:
            try:
                folder = f"extracted_images/{tenant_id}/{ref}"
                objects = await asyncio.to_thread(supabase.storage.from_("files").list, folder)
                paths = [
                    f"{folder}/{obj['name']}"
                    for obj in (objects or [])
                    if isinstance(obj, dict) and obj.get("name")
                ]
                for pbatch in _chunked(paths, 100):
                    await asyncio.to_thread(supabase.storage.from_("files").remove, pbatch)
            except Exception as e:
                logger.warning("[knowledge_files] bulk delete extracted_images (%s) failed: %s", ref, e)
        result["storage_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete storage failed: %s", e)

    # 7. knowledge_files row 삭제 (source_ref IN refs)
    try:
        for batch in _chunk_by_urllen(refs):
            await asyncio.to_thread(
                supabase.table("knowledge_files")
                .delete()
                .eq("tenant_id", tenant_id)
                .in_("source_ref", batch)
                .execute
            )
        result["knowledge_rows_deleted"] = True
    except Exception as e:
        logger.warning("[knowledge_files] bulk delete knowledge_files rows failed: %s", e)

    logger.info(
        "[knowledge_files] delete_entries_bulk tenant=%s files=%d chunks=%d : %s",
        tenant_id, len(refs), len(chunk_ids), result,
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
    _fname = row.get("file_name") or ""  # path 컬럼 동기화용 (rename select 에 file_name 포함)

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
                .update({"folder_path": new_folder_path, "path": compose_path(new_folder_path, _fname)})
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
            .update({"folder_path": new_folder_path, "source_ref": new_ref, "path": compose_path(new_folder_path, _fname)})
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
            .select("source_ref, folder_path, file_name")
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
            .select("source_ref, folder_path, file_name")
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


async def grant_folder_permission(
    tenant_id: str, user_id: Optional[str], folder_path: str
) -> None:
    """폴더 생성/업로드 시 *생성자 본인* 에게 조회 권한을 자동 부여한다.

    배경: 폴더 조회 권한(folder_permissions)은 관리자가 부여하는데, 일반 사용자가
    직접 만든/올린 폴더는 권한이 없어 새로고침하면 본인에게도 안 보이던 결함이 있었다.
    생성 시점에 본인 권한을 1행 자동 upsert 해 "내가 만든 건 내가 본다"를 보장한다.
    (멱등 upsert. 관리자는 어차피 전체 조회라 무해. 서비스롤이라 RLS 영향 없음.)
    """
    fp = (folder_path or "").strip().strip("/")
    if not tenant_id or not user_id or not fp:
        return
    try:
        await asyncio.to_thread(
            supabase.table("folder_permissions")
            .upsert(
                {"tenant_id": tenant_id, "user_id": user_id, "folder_path": fp},
                on_conflict="tenant_id,user_id,folder_path",
            )
            .execute
        )
    except Exception as e:
        logger.warning("[knowledge_files] grant_folder_permission failed (%s/%s): %s", user_id, fp, e)


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
                "uploaded_by_uid, uploaded_by_name, doc_role"
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


# 프론트 목록/모달이 쓰는 knowledge_files 조회 필드(요약 상태만 평탄화, 무거운 doc_card 전체는 제외)
_KF_LIST_SELECT = (
    "source_type, source_ref, file_name, folder_path, path, drive_folder_id, "
    "mime_type, size_bytes, modified_time, owner, "
    "uploaded_by_uid, uploaded_by_name, index_status, "
    "index_error, indexed_at, updated_at, doc_role, "
    "abstract_status:doc_card->>abstract_status, abstract:doc_card->>abstract"
)


async def list_for_tenant(tenant_id: str) -> List[Dict[str, Any]]:
    """테넌트의 *모든* knowledge_files row 반환 (전체 조회 — 대량 테넌트에선 무거우니 폴더 lazy 를 권장).

    성능: doc_card 전체(요약/키포인트/TOC 등 무거운 JSON) 대신 abstract_status/abstract 만 평탄화.
    """
    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select(_KF_LIST_SELECT)
            .eq("tenant_id", tenant_id)
            .order("folder_path", desc=False)
            .order("file_name", desc=False)
            .execute
        )
        # 채팅 첨부(source_ref 가 'session/' 또는 'files/')는 KB 브라우저/폴더트리에 안 뜨게 제외.
        # (에이전트의 카탈로그/페이지 읽기는 file_id 로 직접 접근하므로 영향 없음)
        return [r for r in (result.data or []) if not _is_chat_attachment_ref(r.get("source_ref"))]
    except Exception as e:
        logger.warning("[knowledge_files] list_for_tenant failed: %s", e)
        return []


async def list_counts(tenant_id: str) -> Dict[str, Any]:
    """가벼운 카운트 집계 — 폴더 lazy 로딩 시 트리 배지/역할 탭 카운트용.

    파일 전체 행(무거운 abstract 등) 대신 (folder_path, doc_role, index_status) 3개 컬럼만 읽어
    role별 총계 / role별 폴더 직속 파일수 / 상태별 총계를 서버에서 집계해 *작은 JSON* 으로 반환한다.
    (수만 건이어도 3컬럼이라 전체 조회보다 훨씬 가볍고, 프론트는 수만 항목을 렌더하지 않음)
    """
    role_totals: Dict[str, int] = {}
    folder_direct: Dict[str, Dict[str, int]] = {}
    # 인덱싱 완료(indexed) 파일만의 role별 폴더 직속 카운트 — 채팅 모달(선택 가능한 파일만 노출)에서
    # 폴더 체크박스의 '전체 선택됨' 판정 기준. folder_direct 는 모든 상태 포함(목록 페이지 배지용).
    folder_direct_indexed: Dict[str, Dict[str, int]] = {}
    status_totals: Dict[str, int] = {}
    try:
        rows = (await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("folder_path, doc_role, index_status, source_ref")
            .eq("tenant_id", tenant_id).limit(200000).execute
        )).data or []
    except Exception as e:
        logger.warning("[knowledge_files] list_counts failed: %s", e)
        return {"role_totals": {}, "folder_direct": {}, "folder_direct_indexed": {}, "status_totals": {}}
    for r in rows:
        # 채팅 첨부는 카운트에서 제외 (KB 브라우저 배지/역할탭 오염 방지)
        if _is_chat_attachment_ref(r.get("source_ref")):
            continue
        role = (r.get("doc_role") or "content")
        st = r.get("index_status") or "unknown"
        fp = (r.get("folder_path") or "").strip().strip("/")
        role_totals[role] = role_totals.get(role, 0) + 1
        status_totals[st] = status_totals.get(st, 0) + 1
        if fp:
            d = folder_direct.setdefault(role, {})
            d[fp] = d.get(fp, 0) + 1
            if st == "indexed":
                di = folder_direct_indexed.setdefault(role, {})
                di[fp] = di.get(fp, 0) + 1
    return {
        "role_totals": role_totals,
        "folder_direct": folder_direct,
        "folder_direct_indexed": folder_direct_indexed,
        "status_totals": status_totals,
    }


async def list_for_folder(
    tenant_id: str, folder_path: str, recursive: bool = False
) -> List[Dict[str, Any]]:
    """*폴더 단위* knowledge_files 조회 — lazy 로딩용(수만 건 테넌트에서 전체 조회 회피).

    - recursive=False: 그 폴더에 *직접* 든 파일만 (목록 페이지 표시용).
    - recursive=True : 그 폴더 + 모든 하위 파일 (모달에서 폴더 선택 → 파일 refs 해결용).
    prefix LIKE 파싱 안전을 위해 exact/prefix 를 분리 질의 후 병합(list_files_in_folder_recursive 와 동일 패턴).
    """
    fp = (folder_path or "").strip().strip("/")
    if not tenant_id or not fp:
        return []
    rows: List[Dict[str, Any]] = []
    try:
        exact = await asyncio.to_thread(
            supabase.table("knowledge_files").select(_KF_LIST_SELECT)
            .eq("tenant_id", tenant_id).eq("folder_path", fp)
            .order("file_name", desc=False).execute
        )
        rows.extend(exact.data or [])
    except Exception as e:
        logger.warning("[knowledge_files] list_for_folder exact failed: %s", e)
    if recursive:
        try:
            child = await asyncio.to_thread(
                supabase.table("knowledge_files").select(_KF_LIST_SELECT)
                .eq("tenant_id", tenant_id).like("folder_path", f"{fp}/%")
                .order("folder_path", desc=False).order("file_name", desc=False).execute
            )
            rows.extend(child.data or [])
        except Exception as e:
            logger.warning("[knowledge_files] list_for_folder children failed: %s", e)
    return rows


async def fetch_rows_by_folders(
    tenant_id: str,
    select_cols: str,
    folder_paths: List[str],
    *,
    doc_role: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """여러 폴더의 *subtree* row 를 폴더 경로로 직접 조회(폴더 스코프).

    수천 개 file_id(source_ref=긴 한글 경로)를 URL IN 절로 나열하면 kong/PostgREST URL 한계를
    넘겨 조회가 통째로 실패한다. 폴더 선택 시에는 file_id 열거 대신 folder_path(few)로 직접
    eq+like 질의해 스코프한다. 폴더당 exact + ``folder/%`` 프리픽스 2질의를 병합, source_ref 로 dedup.
    """
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for raw in folder_paths or []:
        p = (raw or "").strip().strip("/")
        if not p:
            continue
        for like in (None, f"{p}/%"):
            try:
                q = (
                    supabase.table("knowledge_files").select(select_cols)
                    .eq("tenant_id", tenant_id)
                )
                q = q.eq("folder_path", p) if like is None else q.like("folder_path", like)
                if doc_role:
                    q = q.eq("doc_role", doc_role)
                if limit is not None:
                    q = q.limit(limit)
                rows = (await asyncio.to_thread(q.execute)).data or []
            except Exception as e:
                logger.warning("[knowledge_files] fetch_rows_by_folders(%r,%s) failed: %s", p, like, e)
                continue
            for r in rows:
                ref = r.get("source_ref")
                if ref is not None:
                    if ref in seen:
                        continue
                    seen.add(str(ref))
                out.append(r)
    return out


async def search_by_name(
    tenant_id: str, q: str, indexed_only: bool = False, limit: int = 300
) -> List[Dict[str, Any]]:
    """파일명 부분일치 검색 — 채팅 모달의 lazy 트리에서 '전체 로드 없이' 검색을 지원.

    수만 건 테넌트에서 전체를 프론트로 내리지 않고, file_name ILIKE 로 서버에서 좁혀 상위 N건만 반환한다.
    indexed_only=True 면 선택 가능한(인덱싱 완료) 파일만.
    """
    term = (q or "").strip()
    if not tenant_id or not term:
        return []
    # PostgREST ilike 와일드카드 — 특수문자(%,_,,)는 이스케이프해 리터럴 매칭
    safe = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace(",", " ")
    try:
        query = (
            supabase.table("knowledge_files").select(_KF_LIST_SELECT)
            .eq("tenant_id", tenant_id).ilike("file_name", f"%{safe}%")
        )
        if indexed_only:
            query = query.eq("index_status", "indexed")
        result = await asyncio.to_thread(
            query.order("file_name", desc=False).limit(limit).execute
        )
        return list(result.data or [])
    except Exception as e:
        logger.warning("[knowledge_files] search_by_name failed: %s", e)
        return []
