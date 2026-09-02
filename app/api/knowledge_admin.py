"""내부 지식공간 파일 관리 API — 설정 페이지에서 직접 업로드/삭제."""
from __future__ import annotations

import asyncio
import hashlib
import io
import logging
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Query, UploadFile

from app.services.document_processor import get_document_processor
from app.services.knowledge_files import (
    INDEX_STATUS_FAILED,
    INDEX_STATUS_INDEXED,
    INDEX_STATUS_PENDING,
    INDEX_STATUS_PROCESSING,
    clear_index_artifacts,
    create_folder as kf_create_folder,
    delete_entry,
    delete_entries_bulk,
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


async def _resolve_admin(requester_uid: Optional[str], tenant_id: str) -> bool:
    """Supabase users 테이블에서 실제 관리자 여부를 조회한다.

    클라이언트가 전달한 is_admin 값을 신뢰하지 않고 서버에서 직접 확인한다.

    Args:
        requester_uid: 요청자의 사용자 ID.
        tenant_id: 테넌트 ID.

    Returns:
        사용자가 관리자이면 `True`, 아니면 `False`.
    """
    if not requester_uid:
        return False
    try:
        from app.core.supabase_client import supabase

        result = await asyncio.to_thread(
            lambda: supabase.table("users")
            .select("is_admin, role")
            .eq("id", requester_uid)
            .eq("tenant_id", tenant_id)
            .maybe_single()
            .execute()
        )
        if result.data:
            return bool(result.data.get("is_admin")) or result.data.get("role") == "superAdmin"
    except Exception as e:
        logger.warning("[knowledge_admin] admin check failed uid=%s tenant=%s: %s", requester_uid, tenant_id, e)
    return False


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


async def _index_uploaded_file(
    *,
    tenant_id: str,
    storage_path: str,
    file_content: bytes,
    file_name: str,
    doc_role: Optional[str],
    public_url: Optional[str] = None,
) -> Optional[str]:
    """스토리지에 올라온 파일을 파싱·인덱싱하고 ``index_status`` 를 갱신한다 (upload/reindex 공용).

    반환: 실패 사유 문자열(str) 또는 ``None``(성공). 실패 시 ``mark_status(FAILED)`` 까지 처리.

    ── doc_role 별 인덱싱 정책 ──
      content/reference : 풀 파이프라인 (pages + abstract + 청킹 + 임베딩)
      glossary/template : 페이지만 저장 (abstract·청킹·임베딩 skip)
      dataset           : 페이지·청킹·임베딩 skip, workbook_card 만 추출해 doc_card 저장
    """
    from app.services.knowledge_files import normalize_doc_role  # 동일 정규화 사용
    role_norm = normalize_doc_role(doc_role)
    skip_chunking_and_embedding = role_norm in ("glossary", "template", "dataset")
    skip_abstract = role_norm in ("glossary", "template", "dataset")
    skip_page_extraction = role_norm == "dataset"  # dataset 은 페이지 단위 자체가 의미 없음

    file_extension = Path(file_name).suffix.lower()
    image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
    is_image = file_extension in image_extensions

    documents = []
    indexing_error: Optional[str] = None
    try:
        # 멱등성: 재시도/재시작 복구/재인덱싱 시 이전(부분) 산출물을 먼저 정리해 청크 중복 방지.
        # 최초 인덱싱이면 사실상 no-op(인덱스 조회라 저렴). 스토리지 원본/knowledge_files row 는 유지.
        await clear_index_artifacts(tenant_id, storage_path)

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
        elif skip_page_extraction:
            # dataset role — 페이지/청킹/임베딩 모두 skip. workbook_card 만 추출해 doc_card 에 저장.
            # 아래 `if skip_chunking_and_embedding:` 블록이 mark_status(indexed) 처리.
            from app.services.workbook_card import extract_workbook_card
            from app.core.supabase_client import supabase
            card = extract_workbook_card(file_content, file_name)
            try:
                await asyncio.to_thread(
                    supabase.table("knowledge_files")
                        .update({"doc_card": card})
                        .eq("tenant_id", tenant_id)
                        .eq("source_type", "upload")
                        .eq("source_ref", storage_path)
                        .execute
                )
                logger.info(
                    "[knowledge_admin] %s: dataset ingest (workbook_card saved, n_sheets=%d)",
                    file_name, len(card.get("sheets", [])),
                )
            except Exception as card_err:
                logger.warning(
                    "[knowledge_admin] %s: workbook_card save failed: %s",
                    file_name, card_err,
                )
            documents = []
        else:
            file_io = io.BytesIO(file_content)
            processor = get_document_processor()
            docs = await processor.load_document(file_io, file_name)
            if docs:
                # 페이지 단위 저장 — 모든 role 에서 수행. glossary/template 는 abstract LLM skip.
                # file_id 는 storage_path 와 동일 (chunk metadata.file_id 와도 일치).
                # 실패는 격리 — 청크 RAG 파이프라인은 계속.
                try:
                    from app.services.document_pages import post_load_hook
                    await post_load_hook(
                        tenant_id, storage_path, docs,
                        skip_abstract=skip_abstract,
                    )
                except Exception as page_err:
                    logger.warning(
                        "[knowledge_admin] post_load_hook failed for %s: %s",
                        file_name, page_err,
                    )

                if not skip_chunking_and_embedding:
                    if role_norm == "legal_review":
                        # 검토 사례 — 조항 단위 구조화 청크(사업배경 1개 + 조항 N개, 메모 동승).
                        # 일반 청킹(process_documents) 대신 전용 구조화기 사용.
                        from app.services.legal_review import build_legal_review_documents
                        documents = await asyncio.to_thread(
                            build_legal_review_documents,
                            file_content, file_name, storage_path, tenant_id,
                        )
                        logger.info(
                            "[knowledge_admin] %s: legal_review 구조화 (chunks=%d)",
                            file_name, len(documents or []),
                        )
                    else:
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

        if skip_chunking_and_embedding:
            # glossary/template — 페이지 저장만으로 인덱싱 완료 처리.
            # processed_files 에는 저장 안 함 (RAG 청크 매칭 키라서 의미 없음).
            #
            # ── glossary 추가 단계: 페이지 본문에서 LLM 으로 용어 매핑 정제 추출 ──
            # 추출본은 doc_card.glossary_compact 에 저장. /glossary/inline 이 우선 활용.
            # 실패해도 인덱싱은 indexed 로 남김 (raw page fallback 동작).
            if role_norm == "glossary":
                # ── 우선: 고정형 CSV(영문,한글뜻,약어) 는 구조화 테이블(glossary_terms)로 직행 ──
                # LLM 정제 없이 행 그대로 저장 → rfi-translate 등 소비자가 term-lock 으로 활용.
                # 구조화 파싱이 성립하면 compact 추출은 건너뛴다(중복/토큰낭비 방지).
                structured_done = False
                try:
                    from app.services.glossary_terms import (
                        parse_glossary_terms,
                        replace_file_terms,
                    )
                    parsed_terms = parse_glossary_terms(file_content, file_name)
                    if parsed_terms is not None:
                        n_terms = await replace_file_terms(
                            tenant_id, storage_path, file_name, parsed_terms,
                        )
                        structured_done = True
                        logger.info(
                            "[knowledge_admin] %s: structured glossary imported "
                            "(%d terms → glossary_terms)",
                            file_name, n_terms,
                        )
                except Exception as struct_err:
                    logger.warning(
                        "[knowledge_admin] %s: structured glossary parse failed "
                        "(%s) — fallback: LLM compact 추출",
                        file_name, struct_err,
                    )

                try:
                    if structured_done:
                        gloss_result = {"saved": False, "structured": True}
                    else:
                        from app.services.glossary_extraction import extract_and_save_glossary_compact
                        gloss_result = await extract_and_save_glossary_compact(
                            tenant_id=tenant_id, file_id=storage_path,
                        )
                    if gloss_result.get("structured"):
                        pass  # 구조화 저장 완료 — 위에서 이미 로깅.
                    elif gloss_result.get("saved"):
                        logger.info(
                            "[knowledge_admin] %s: glossary extracted & saved "
                            "(terms=%d, ok_batches=%d/%d)",
                            file_name,
                            gloss_result.get("term_count", 0),
                            gloss_result.get("ok_batches", 0),
                            gloss_result.get("n_batches", 0),
                        )
                    else:
                        logger.warning(
                            "[knowledge_admin] %s: glossary extraction failed/empty "
                            "(errors=%s) — fallback: /glossary/inline 이 raw page 사용",
                            file_name, (gloss_result.get("errors") or [])[:3],
                        )
                except Exception as gloss_err:
                    logger.warning(
                        "[knowledge_admin] %s: glossary extraction crashed: %s "
                        "— fallback: /glossary/inline 이 raw page 사용",
                        file_name, gloss_err,
                    )

            await mark_status(
                tenant_id=tenant_id,
                source_type="upload",
                source_ref=storage_path,
                status=INDEX_STATUS_INDEXED,
            )
            logger.info(
                "[knowledge_admin] %s: pages-only ingest (role=%s, skipped chunking/embedding)",
                file_name, role_norm,
            )
        elif documents:
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

    # terminal 실패 상태(failed)는 *큐가 소유*한다 — 여기서 failed 로 마킹하지 않는다.
    # (transient 를 여기서 failed 로 찍으면 재시도 때 깜빡이고, 그 사이 크래시 시 failed 로 굳어
    #  pending/processing 만 복구하는 sweeper 가 못 살림. 성공 시 indexed 마킹은 위에서 이미 수행.)
    return indexing_error


@router.post("/knowledge/files/upload")
async def upload_knowledge_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    tenant_id: str = Form(...),
    folder_path: Optional[str] = Form(None),
    file_hash: Optional[str] = Form(None),
    uploaded_by_uid: Optional[str] = Form(None),
    uploaded_by_name: Optional[str] = Form(None),
    doc_role: Optional[str] = Form(None),
):
    """설정 페이지에서 내부 지식공간 파일을 직접 업로드.

    실물 파일은 Supabase Storage 'files' 버킷의 'knowledge/{tenant}/' 아래에 저장하고,
    knowledge_files 테이블에 source_type='upload'로 등록한 후 RAG 인덱싱한다.
    """
    file_content = await file.read()
    # 폴더 업로드(webkitdirectory) 등에서 filename 이 *전체 상대경로* 로 오는 경우가 있다
    # (예: 'mock-corpus/A_.../01.선순위/Credit Agreement.pdf'). 폴더 구조는 folder_path 로 따로
    # 저장하므로 file_name 은 항상 *basename* 으로 정규화한다 — 그래야 grep/page 의 file_name
    # 해석(_resolve_file_id)이 동작한다. 클라이언트가 무엇을 보내든 방어(서버가 단일 진실).
    file_name = (file.filename or "unknown").replace("\\", "/").rstrip("/").split("/")[-1] or "unknown"
    size_bytes = len(file_content)

    # doc_role(분류)별 허용 확장자 정책 — storage 업로드 전에 거부해 orphan 파일 방지.
    #   content/glossary/reference: pdf/hwp/hwpx/doc/docx/pptx/txt
    #   template(양식): hwpx/docx · dataset(데이터): xlsx · legal_review(검토 사례): docx
    from app.services.knowledge_files import (
        normalize_doc_role as _norm_role_early,
        allowed_extensions_for_role as _allowed_exts,
        is_extension_allowed_for_role as _ext_ok,
    )
    if not _ext_ok(file_name, doc_role):
        _allowed = ", ".join(_allowed_exts(doc_role))
        _hint = (
            " (검토 사례는 변호사 메모 추출이 docx XML 한정)"
            if _norm_role_early(doc_role) == "legal_review"
            else ""
        )
        raise HTTPException(
            status_code=400,
            detail=f"이 분류에서는 {_allowed} 형식만 업로드할 수 있습니다{_hint}.",
        )

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

    # 2) knowledge_files 사전 등록 (pending — 인덱싱은 백그라운드 워커풀이 처리)
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
        doc_role=doc_role,
        initial_status=INDEX_STATUS_PENDING,
    )

    # 2-1) knowledge_folders 레지스트리에도 폴더(+조상) 등록.
    # "+폴더" 버튼으로 만든 폴더처럼, *업로드*로 생긴 폴더도 레지스트리에 남긴다(불일치 제거).
    # 폴더 업로드 시 nested 경로의 모든 단계를 등록해 트리와 일치시킨다. idempotent upsert.
    _fp = (folder_path or "").strip().strip("/")
    if _fp:
        _acc: list[str] = []
        for _seg in [s for s in _fp.split("/") if s]:
            _acc.append(_seg)
            try:
                await kf_create_folder(
                    tenant_id=tenant_id, folder_path="/".join(_acc), doc_role=doc_role
                )
            except Exception as _e:
                logger.warning("[knowledge_admin] folder register failed (%s): %s", "/".join(_acc), _e)

    # 3) 인덱싱은 백그라운드 워커풀에 위임하고 즉시 반환한다.
    #    (브라우저가 인덱싱 끝까지 연결을 붙잡지 않음 → 대량 폴더 업로드도 수 분 내 접수 완료.
    #     실제 인덱싱 진행은 프론트가 index_status 폴링으로 확인. 폴더 카드 갱신도 인덱싱 완료 후.)
    from app.services.ingest_queue import enqueue_index_job
    enqueue_index_job(tenant_id, storage_path, file_name, doc_role, folder_path or "")

    return {
        "source_type": "upload",
        "source_ref": storage_path,
        "file_name": file_name,
        "size_bytes": size_bytes,
        "public_url": public_url,
        "accepted": True,
        "status": INDEX_STATUS_PENDING,   # 인덱싱은 백그라운드 진행 — 프론트는 상태 폴링
    }


@router.get("/knowledge/ingest/status")
async def knowledge_ingest_status(tenant_id: str = Query(...)):
    """백그라운드 인제스트 관측 — 테넌트 upload 파일 상태별 카운트 + 큐/동시성 스냅샷."""
    from app.services.ingest_queue import ingest_status_counts
    return await ingest_status_counts(tenant_id)


@router.get("/knowledge/files/counts")
async def knowledge_files_counts(tenant_id: str = Query(...)):
    """폴더 lazy 로딩용 경량 카운트 — role별 총계 / role별 폴더 직속 파일수 / 상태별 총계."""
    from app.services.knowledge_files import list_counts
    return await list_counts(tenant_id)


@router.get("/knowledge/files/search")
async def knowledge_files_search(
    tenant_id: str = Query(...),
    q: str = Query(...),
    indexed_only: bool = Query(False),
    limit: int = Query(300),
):
    """파일명 부분일치 검색 — lazy 트리에서 전체 로드 없이 검색. 상위 N건만 반환."""
    from app.services.knowledge_files import search_by_name
    rows = await search_by_name(tenant_id, q, indexed_only=indexed_only, limit=limit)
    return {"file_details": rows}


@router.post("/knowledge/files/reindex")
async def reindex_knowledge_file(
    tenant_id: str = Form(...),
    source_ref: str = Form(...),
    source_type: str = Form("upload"),
    requester_uid: Optional[str] = Form(None),
):
    """인덱싱 실패(``index_status='failed'``) 등인 업로드 파일을 *재인덱싱*.

    스토리지의 원본을 다시 받아 업로드와 동일 파이프라인(``_index_uploaded_file``)으로 재처리.
    임베딩 일시 과부하(429) 등 일시적 실패의 후속조치. 드라이브 소스는 미지원(원본이 외부).
    권한: 관리자 또는 업로더 본인.
    """
    if source_type != "upload":
        raise HTTPException(status_code=400, detail="재인덱싱은 업로드 파일만 지원합니다.")
    entry = await get_entry(tenant_id, source_type, source_ref)
    if not entry:
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다.")

    # 권한 — 관리자이거나 본인이 올린 파일만 (삭제 권한 규칙과 동일)
    is_admin = await _resolve_admin(requester_uid, tenant_id)
    is_owner = bool(requester_uid) and str(entry.get("uploaded_by_uid") or "") == str(requester_uid)
    if not (is_admin or is_owner):
        raise HTTPException(status_code=403, detail="재인덱싱 권한이 없습니다 (관리자 또는 업로더 본인).")

    file_name = entry.get("file_name") or Path(source_ref).name
    doc_role = entry.get("doc_role")

    # 재인덱싱도 백그라운드 워커풀에 위임 — pending 으로 돌리고 enqueue(워커가 멱등 정리 후 재처리).
    await mark_status(
        tenant_id=tenant_id, source_type=source_type, source_ref=source_ref,
        status=INDEX_STATUS_PENDING,
    )
    from app.services.ingest_queue import enqueue_index_job
    enqueue_index_job(tenant_id, source_ref, file_name, doc_role, entry.get("folder_path") or "")
    return {
        "source_type": source_type,
        "source_ref": source_ref,
        "file_name": file_name,
        "accepted": True,
        "status": INDEX_STATUS_PENDING,
    }


@router.post("/knowledge/files/resummarize")
async def resummarize_knowledge_file(
    background_tasks: BackgroundTasks,
    tenant_id: str = Form(...),
    source_ref: str = Form(...),
    source_type: str = Form("upload"),
    requester_uid: Optional[str] = Form(None),
):
    """파일의 *요약(abstract)만* 재생성한다 — 재파싱·재임베딩 없이 저장된 페이지 재사용.

    요약 실패(``doc_card.abstract_status='failed'``)한 파일의 '다시 요약' 버튼용. 완료 후
    해당 폴더 카드도 재생성(요약이 폴더 카드의 입력이므로). 권한: 관리자 또는 업로더 본인.
    """
    from app.core.supabase_client import supabase
    from app.services.knowledge_files import normalize_doc_role
    from app.services.document_pages import update_doc_card
    from langchain.schema import Document

    entry = await get_entry(tenant_id, source_type, source_ref)
    if not entry:
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다.")

    is_admin = await _resolve_admin(requester_uid, tenant_id)
    is_owner = bool(requester_uid) and str(entry.get("uploaded_by_uid") or "") == str(requester_uid)
    if not (is_admin or is_owner):
        raise HTTPException(status_code=403, detail="재요약 권한이 없습니다 (관리자 또는 업로더 본인).")

    role = normalize_doc_role(entry.get("doc_role"))
    if role in ("glossary", "template", "dataset"):
        raise HTTPException(status_code=400, detail="이 분류는 요약을 생성하지 않습니다.")

    # 저장된 페이지 재사용 (재파싱/재임베딩 없음)
    try:
        res = await asyncio.to_thread(
            supabase.table("document_pages")
            .select("page_number, content")
            .eq("tenant_id", tenant_id)
            .eq("file_id", source_ref)
            .order("page_number", desc=False)
            .execute
        )
        rows = list(res.data or [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"페이지 조회 실패: {e}")
    if not rows:
        raise HTTPException(status_code=409, detail="저장된 페이지가 없습니다. 먼저 재인덱싱하세요.")

    page_docs = [
        Document(page_content=(r.get("content") or ""), metadata={"page_number": r.get("page_number")})
        for r in rows
    ]
    await update_doc_card(tenant_id, source_ref, page_docs)

    # 요약은 폴더 카드의 입력 → 해당 폴더(+조상) 카드 재생성 (백그라운드, fire-and-forget)
    folder_path = (entry.get("folder_path") or "").strip().strip("/")
    if folder_path:
        from app.services.folder_cards import rebuild_folders
        background_tasks.add_task(rebuild_folders, tenant_id, [folder_path], role)

    # 실제 요약 성공 여부 재확인 (doc_card 직접 조회)
    summarized = False
    abstract_status = "failed"
    try:
        chk = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("doc_card")
            .eq("tenant_id", tenant_id)
            .eq("source_ref", source_ref)
            .limit(1)
            .execute
        )
        card = ((chk.data or [{}])[0] or {}).get("doc_card") or {}
        if isinstance(card, dict):
            summarized = bool(card.get("abstract"))
            abstract_status = card.get("abstract_status") or ("done" if summarized else "failed")
    except Exception as e:
        logger.warning("[knowledge_admin] resummarize verify failed: %s", e)

    return {
        "source_ref": source_ref,
        "summarized": summarized,
        "abstract_status": abstract_status,
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
):
    """파일 1개를 RAG 인덱스 + storage + knowledge_files에서 완전 제거.

    권한:
      - 관리자(DB의 is_admin=true 또는 role=superAdmin): 모든 파일 삭제 가능
      - 일반 사용자: 본인이 업로드한 'upload' 파일만 삭제 가능
      - drive 인덱스 항목: 관리자만 제거 가능

    클라이언트가 전달하는 is_admin 값은 사용하지 않으며, 서버에서 DB를 직접 조회해 권한을 확인한다.
    """
    if source_type not in {"drive", "upload"}:
        raise HTTPException(status_code=400, detail="invalid source_type")

    is_admin = await _resolve_admin(requester_uid, tenant_id)

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
    """빈 폴더 포함 등록된 모든 폴더 row 반환 ([{folder_path, doc_role}, ...])."""
    folders = await list_folders_for_tenant(tenant_id)
    return {"folders": folders}


@router.post("/knowledge/folders/build-cards")
async def build_folder_cards(
    background_tasks: BackgroundTasks,
    tenant_id: str = Form(...),
    doc_role: Optional[str] = Form(None),
    requester_uid: Optional[str] = Form(None),
    background: bool = Form(True),
):
    """폴더 카드 일괄 백필 — tenant 의 모든 폴더를 bottom-up 으로 1회 생성(멱등).

    벌크 업로드 후 1회 실행하는 용도. signature 동일 폴더는 skip 하므로 재실행 안전.
    폴더당 LLM 1회라 문서 수와 무관하게 비용 bounded. (관리자 전용)

    Args:
        doc_role: 옵션 — 특정 role 만. 미지정 시 등장한 모든 role.
        background: True(기본)면 백그라운드 실행 후 즉시 응답. False 면 끝까지 기다려 요약 반환.
    """
    is_admin = await _resolve_admin(requester_uid, tenant_id)
    if not is_admin:
        raise HTTPException(status_code=403, detail="관리자만 폴더 카드를 생성할 수 있습니다.")

    from app.services.folder_cards import backfill_tenant

    if background:
        background_tasks.add_task(backfill_tenant, tenant_id, doc_role)
        return {"ok": True, "scheduled": True, "tenant_id": tenant_id, "doc_role": doc_role}

    result = await backfill_tenant(tenant_id, doc_role)
    return {"ok": True, "scheduled": False, **result}


@router.post("/knowledge/folders")
async def create_knowledge_folder(
    tenant_id: str = Form(...),
    folder_path: str = Form(...),
    doc_role: Optional[str] = Form(None),
    requester_uid: Optional[str] = Form(None),
):
    """빈 폴더 생성 (knowledge_folders에 row 추가). doc_role 미지정 시 'content'."""
    folder_path = (folder_path or "").strip().strip("/")
    if not folder_path:
        raise HTTPException(status_code=400, detail="folder_path required")
    ok = await kf_create_folder(
        tenant_id=tenant_id, folder_path=folder_path, doc_role=doc_role
    )
    return {"ok": ok, "folder_path": folder_path}


@router.post("/knowledge/folders/refresh-cards")
async def refresh_folder_cards(
    background_tasks: BackgroundTasks,
    tenant_id: str = Form(...),
    folder_paths: List[str] = Form([]),
    doc_role: Optional[str] = Form(None),
):
    """업로드/삭제 *배치 후* 영향받은 폴더 카드만 갱신 — storm 없는 자동 경로.

    프론트가 한 배치(업로드 N파일/삭제 N파일)가 끝난 뒤 *1회* 호출한다.
    - ``folder_paths`` 주면 그 폴더들 + 조상만 bottom-up 재생성(``rebuild_folders``).
    - 비우면 tenant 전체 backfill.
    signature-skip 이라 실제 안 바뀐 폴더는 LLM 호출 없음. 백그라운드 실행 후 즉시 응답.
    (멱등·파생 데이터라 관리자 게이트 없음. 안 바뀌면 비용 0이라 남용 위험도 낮음.)
    """
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")
    from app.services.folder_cards import backfill_tenant, rebuild_folders

    cleaned = [str(p).strip().strip("/") for p in (folder_paths or []) if str(p or "").strip().strip("/")]
    if cleaned:
        background_tasks.add_task(rebuild_folders, tenant_id, cleaned, doc_role or "content")
    else:
        background_tasks.add_task(backfill_tenant, tenant_id, doc_role)
    return {"ok": True, "scheduled": True, "folders": (len(cleaned) or "all")}


@router.post("/knowledge/folders/rename")
async def rename_knowledge_folder(
    tenant_id: str = Form(...),
    old_path: str = Form(...),
    new_path: str = Form(...),
    requester_uid: Optional[str] = Form(None),
    doc_role: Optional[str] = Form(None),
):
    """업로드 폴더 이름 변경 — 하위 경로도 prefix 치환 (role scope). (관리자 전용)"""
    is_admin = await _resolve_admin(requester_uid, tenant_id)
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
        doc_role=doc_role,
    )
    return {"ok": True, "affected": affected}


@router.delete("/knowledge/folders")
async def delete_knowledge_folder(
    tenant_id: str = Query(...),
    folder_path: str = Query(...),
    requester_uid: Optional[str] = Query(None),
    doc_role: Optional[str] = Query(None),
):
    """업로드 폴더 삭제 — 하위 모든 파일을 storage/RAG/knowledge_files에서 영구 제거 (role scope). (관리자 전용)"""
    is_admin = await _resolve_admin(requester_uid, tenant_id)
    if not is_admin:
        raise HTTPException(status_code=403, detail="관리자만 폴더를 삭제할 수 있습니다.")
    folder_path = (folder_path or "").strip()
    if not folder_path:
        raise HTTPException(status_code=400, detail="folder_path required")

    rows = await list_files_in_folder_recursive(
        tenant_id=tenant_id,
        folder_path=folder_path,
        source_type="upload",
        doc_role=doc_role,
    )
    # 파일별 delete_entry 루프(파일당 풀스캔 N회)는 대량 폴더에서 폭발 → 집합 기반 일괄 삭제로 대체.
    entries = [
        {"source_type": r.get("source_type") or "upload", "source_ref": r.get("source_ref") or ""}
        for r in rows
        if r.get("source_ref")
    ]
    bulk = await delete_entries_bulk(tenant_id=tenant_id, entries=entries)

    # 실제 결과를 반영 — 예전엔 무조건 ok:True 라, IN-리스트 URL 초과로 삭제가 통째로 실패해도
    # '성공(deleted=total)'으로 오인됐다(파일 100+ 폴더). 이제 knowledge_files row 삭제 성공
    # 여부(목록에 보이는 실체)로 판정하고, 실패 시 폴더 메타/카드도 건드리지 않는다.
    rows_ok = (not entries) or bool(bulk.get("knowledge_rows_deleted"))
    artifacts_ok = (not entries) or all(
        bulk.get(k)
        for k in (
            "documents_deleted", "image_analysis_documents_deleted", "chroma_deleted",
            "document_images_deleted", "pages_deleted", "processed_files_deleted",
            "storage_deleted",
        )
    )
    deleted = len(rows) if rows_ok else 0
    failed = 0 if rows_ok else len(rows)

    if rows_ok:
        # knowledge_folders 메타 row도 정리 (빈 폴더 + 자식 폴더, role scope)
        await delete_folder_meta(tenant_id=tenant_id, folder_path=folder_path, doc_role=doc_role)
        # 폴더 카드 정리: 삭제된 폴더 + 하위(subtree) 카드 행 제거 후, *부모* 카드 재생성
        # (부모는 하위폴더 1개가 줄었으므로 요약/카운트 갱신 필요).
        from app.services.folder_cards import delete_folder_cards, rebuild_folders
        await delete_folder_cards(tenant_id=tenant_id, folder_path=folder_path, doc_role=doc_role)
        _parent = folder_path.rsplit("/", 1)[0] if "/" in folder_path else ""
        if _parent:
            await rebuild_folders(tenant_id, [_parent], doc_role or "content")
    else:
        logger.warning(
            "[knowledge_admin] folder delete FAILED to remove rows: tenant=%s folder=%s files=%d bulk=%s",
            tenant_id, folder_path, len(rows), bulk,
        )

    return {
        "ok": rows_ok,
        # 파일은 지워졌지만 RAG/스토리지 아티팩트 일부 실패 → orphan 가능(목록엔 안 보임)
        "partial": rows_ok and not artifacts_ok,
        "deleted": deleted,
        "failed": failed,
        "total": len(rows),
        "detail": bulk,
    }
