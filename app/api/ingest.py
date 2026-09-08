"""인제스트 라우터: /process, /process-output, /save-to-storage."""
from __future__ import annotations

import io
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.core.supabase_client import supabase
from app.converters.form2docx import form_to_docx
from app.converters.markdown import convert_markdown_to_docx
from app.schemas import ProcessOutputRequest, ProcessRequest, ProcessSessionFileRequest
from app.services.document_processor import get_document_processor
from app.services.google_drive_loader import GoogleDriveLoader
from app.services.ingest.image import process_image_file
from app.services.ingest.pipeline import (
    process_database_records,
    process_google_drive,
    process_local_documents,
    process_supabase_storage,
)
from app.services.ingest.state import cleanup_drive_jobs, drive_jobs, tenant_active_job
from app.services.rag_chain import get_rag_chain
from app.storage.supabase_loader import SupabaseStorageLoader

router = APIRouter()


@router.post("/process")
async def process(request: ProcessRequest):
    if request.storage_type == "local":
        return await process_local_documents(request)
    if request.storage_type == "drive":
        return await process_google_drive(request)
    if request.storage_type == "storage":
        return await process_supabase_storage(request)


@router.get("/process/drive/status")
async def get_drive_indexing_status(tenant_id: str):
    """Drive 폴더 인덱싱 잡 폴링용 상태 조회."""
    cleanup_drive_jobs()
    job_id = tenant_active_job.get(tenant_id)
    if job_id and job_id in drive_jobs:
        job = drive_jobs[job_id]
        return {
            "job_id": job_id,
            "status": job["status"],
            "total": job["total"],
            "processed": job["processed"],
            "failed": job["failed"],
            "results": job.get("results"),
            "error": job.get("error"),
        }
    for jid, job in drive_jobs.items():
        if job.get("tenant_id") == tenant_id and job.get("status") in ("completed", "failed"):
            return {
                "job_id": jid,
                "status": job["status"],
                "total": job["total"],
                "processed": job["processed"],
                "failed": job["failed"],
                "results": job.get("results"),
                "error": job.get("error"),
            }
    return {"status": "idle"}


@router.post("/process/database")
async def process_database(request: ProcessRequest):
    return await process_database_records(request)


@router.post("/process-session-file")
async def process_session_file(request: ProcessSessionFileRequest):
    """채팅 세션 첨부 파일 ingest — Supabase storage 에서 받아 청크 + 인덱싱 후 file_id 반환.

    deepagents-lite 의 채팅 첨부 흐름에서 호출. URL 또는 storage path 받음.

    동작:
      1. file_url → storage path 추출 (또는 file_path 직접)
      2. SupabaseStorageLoader 로 download → DocumentProcessor 청크
      3. Storage object key를 stable ``file_id``로 사용
      4. rag.process_and_store_documents 호출 (vector store 인덱싱)
      5. 응답: ``{file_id, file_name, tenant_id, chunks}``
    """
    import os as _os
    from urllib.parse import urlparse

    try:
        # 1) storage path 결정 — file_path 우선, 없으면 URL 에서 추출
        storage_path = (request.file_path or "").strip()
        if not storage_path and request.file_url:
            parsed = urlparse(request.file_url)
            # Supabase: /storage/v1/object/public/{bucket}/{path...}
            parts = parsed.path.split("/storage/v1/object/")
            if len(parts) == 2:
                tail = parts[1]
                # tail 예: "public/files/files/uuid.pdf" → bucket=files 후 "files/uuid.pdf"
                segs = tail.split("/", 2)
                if len(segs) == 3:
                    # segs[0]='public', segs[1]=bucket, segs[2]=path
                    storage_path = segs[2]
        if not storage_path:
            raise HTTPException(status_code=400, detail="file_url 또는 file_path 필수 (URL 파싱 실패)")
        # query string 제거 (?)
        storage_path = storage_path.split("?")[0]

        original_filename = request.file_name or _os.path.basename(storage_path)
        file_extension = Path(original_filename).suffix.lower() or ".bin"

        # 채팅 첨부 허용 확장자 — 지원 문서셋 + 이미지(비전). 그 외는 거부.
        _CHAT_ALLOWED_EXTS = {
            ".pdf", ".hwp", ".hwpx", ".doc", ".docx", ".pptx", ".txt", ".xlsx",
            ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp",
        }
        if file_extension not in _CHAT_ALLOWED_EXTS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"지원하지 않는 파일 형식입니다: {file_extension} "
                    "(허용: pdf/hwp/hwpx/doc/docx/pptx/txt/xlsx/이미지)"
                ),
            )
        # 벡터 인덱싱 제외 — /save-to-storage 와 같은 정책.
        # 호출자가 명시적으로 요청해도 건너뛴다: 첨부를 워크스페이스의 실제 파일로
        # 읽는 쪽은 벡터 검색을 쓰지 않으므로 임베딩이 낭비다.
        skip_vector_index = (
            file_extension in {".xlsx", ".xlsm"} or bool(request.skip_vector_index)
        )

        # 2) Storage object key를 stable file_id로 사용한다.
        # knowledge_files.source_ref는 원본 다운로드 경로이기도 하므로 별도의 session/... 논리 ID를
        # 만들면 /document/raw가 존재하지 않는 객체를 찾게 된다. /save-to-storage와 동일하게 실제
        # object key 하나를 검색·페이지·원본 조회 전 구간의 정본으로 사용한다.
        file_id = storage_path

        # 3) download + 청크
        storage_loader = SupabaseStorageLoader()
        image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]
        is_image = file_extension in image_extensions
        page_docs = None  # 비이미지: 페이지 단위 본문 (document_pages 저장용 — grep/read_document_page 가 읽음)

        if is_image:
            import asyncio as _asyncio
            response = await _asyncio.to_thread(
                storage_loader.supabase.storage.from_("files").download,
                storage_path,
            )
            file_content = response if isinstance(response, bytes) else (
                response.read() if hasattr(response, "read") else bytes(response)
            )
            documents = await process_image_file(
                file_content, original_filename, file_id, request.tenant_id, None, storage_type="storage",
            ) or []
        else:
            # 정상 업로드와 동일하게 *페이지 로드 → 청크* 2단계. download_and_process_file 은 청크만
            # 돌려줘 page_docs 가 사라지므로 직접 로드한다(document_pages 에 페이지 본문을 남기기 위함).
            import asyncio as _asyncio
            raw = await _asyncio.to_thread(
                storage_loader.supabase.storage.from_("files").download,
                storage_path,
            )
            file_bytes = raw if isinstance(raw, bytes) else (
                raw.read() if hasattr(raw, "read") else bytes(raw)
            )
            processor = get_document_processor()
            page_docs = await processor.load_document(io.BytesIO(file_bytes), original_filename)
            if not page_docs:
                raise HTTPException(status_code=400, detail="문서에서 본문 추출 실패 (빈 본문 또는 파싱 실패)")
            if skip_vector_index:
                # save-to-storage 와 같은 정책 — 표 격자는 임베딩하지 않고 텍스트만 남긴다.
                documents = []
                print(f"[process-session-file] {file_extension} → 청킹·임베딩 생략 (텍스트만 보존)")
            else:
                documents = await processor.process_documents(page_docs, {
                    "tenant_id": request.tenant_id,
                    "original_filename": original_filename,
                    "file_path": storage_path,
                    "storage_type": "storage",
                })

        if not documents and not skip_vector_index:
            raise HTTPException(status_code=400, detail="문서에서 청크 추출 실패 (빈 본문 또는 파싱 실패)")

        # 4) 모든 chunk 에 file_id 박음 + doc_role
        doc_role = (request.doc_role or "content").strip().lower()
        for doc in documents:
            try:
                doc.metadata["file_id"] = file_id
                doc.metadata["file_name"] = original_filename
                doc.metadata["doc_role"] = doc_role
                doc.metadata["source_kind"] = "session_attachment"
            except Exception:
                pass

        rag = get_rag_chain()
        if documents:
            ok = await rag.process_and_store_documents(documents, request.tenant_id)
            if not ok:
                raise HTTPException(status_code=500, detail="벡터 저장 실패")

        # 5) knowledge_files 등록 — 에이전트의 카탈로그/경로해석(_resolve_file_id) 이 이 테이블에 의존.
        #    이게 없어서 세션 첨부가 "지식베이스에 등록되어있지 않아" 로 못 읽히던 문제 수정.
        #    source_ref = 세션 file_id(청크 metadata.file_id 와 동일), folder_path="" 로 KB 폴더트리엔 숨김.
        #    upsert(on_conflict) 라 같은 파일 재처리(이중 호출)에도 멱등.
        try:
            from app.services.knowledge_files import register_uploaded_file, INDEX_STATUS_INDEXED
            await register_uploaded_file(
                tenant_id=request.tenant_id,
                storage_path=file_id,
                file_name=original_filename,
                folder_path="",
                initial_status=INDEX_STATUS_INDEXED,
                doc_role=doc_role,
            )
        except Exception as exc:
            print(f"[process-session-file] knowledge_files 등록 실패 (계속): {exc}")

        # 6) document_pages 저장 — read_document_page / grep_in_document 가 벡터스토어가 아니라
        #    이 테이블에서 페이지 본문을 읽는다. (이미지 첨부는 페이지 개념이 없어 skip)
        if page_docs:
            try:
                from app.services.document_pages import post_load_hook
                await post_load_hook(request.tenant_id, file_id, page_docs, skip_abstract=True)
            except Exception as exc:
                print(f"[process-session-file] document_pages 저장 실패 (계속): {exc}")

        # 7) processed_files 테이블에도 등록 (knowledge 조회 호환)
        try:
            await rag.save_processed_files([file_id], request.tenant_id, [original_filename])
        except Exception as exc:
            print(f"[process-session-file] save_processed_files 실패 (계속): {exc}")

        return {
            "file_id": file_id,
            "file_name": original_filename,
            "tenant_id": request.tenant_id,
            "chunks": len(documents),
            "doc_role": doc_role,
        }

    except HTTPException:
        raise
    except Exception as exc:
        print(f"[process-session-file] 실패: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/process-output")
async def process_output(request: ProcessOutputRequest):
    try:
        tenant_id = request.tenant_id

        workitem_id = request.workitem_id
        workitem = supabase.table("todolist").select("*").eq("id", workitem_id).single().execute()
        if not workitem.data:
            raise HTTPException(status_code=404, detail="Workitem not found")

        workitem_data = workitem.data
        activity_name = workitem_data.get("activity_name")
        output = workitem_data["output"]
        form_id = workitem_data["tool"].replace("formHandler:", "")
        form_value = output.get(form_id, {})
        if not tenant_id:
            tenant_id = workitem_data["tenant_id"]

        form_definition = supabase.table("form_def").select("*").eq("id", form_id).eq("tenant_id", tenant_id).single().execute()
        fields_json = form_definition.data.get("fields_json", {})
        form_html = form_definition.data.get("html", "")

        proc_def_id = workitem_data.get("proc_def_id")
        proc_inst_id = workitem_data.get("proc_inst_id")
        today = datetime.now()
        year = f"{today.year:04d}"
        month = f"{today.month:02d}"
        day = f"{today.day:02d}"
        folder_path = f"instances/{proc_def_id}/{year}/{month}/{day}/{proc_inst_id}/output/"

        reports = []
        uploads = []
        drive_loader = GoogleDriveLoader(tenant_id=tenant_id)

        for field in fields_json:
            if field.get("type") == "report" or field.get("type") == "slide":
                field_id = field.get("key")
                if form_value.get(field_id):
                    field_name = field.get("text")
                    file_name = f"{activity_name}_{field_name}.docx"
                    docx_bytes = convert_markdown_to_docx(form_value.get(field_id), file_name)
                    reports.append({
                        "file_content": io.BytesIO(docx_bytes),
                        "file_name": file_name,
                    })

        if len(reports) > 0:
            for report in reports:
                file_name = report.get("file_name")
                file_content = report.get("file_content")
                upload_meta = await drive_loader.save_to_google_drive(
                    file_content=file_content,
                    file_name=file_name,
                    folder_path=folder_path,
                )
                uploads.append(upload_meta)
        else:
            file_name = f"{activity_name}.docx"
            docx_bytes = form_to_docx(form_html, output)
            reports.append({
                "file_content": io.BytesIO(docx_bytes),
                "file_name": file_name,
            })
            upload_meta = await drive_loader.save_to_google_drive(
                file_content=io.BytesIO(docx_bytes),
                file_name=file_name,
                folder_path=folder_path,
            )
            uploads.append(upload_meta)
            try:
                output_url = upload_meta.get("web_view_link")
                supabase.table("todolist").update({"output_url": output_url}).eq("id", workitem_id).execute()
            except Exception as e:
                print(f"Error saving output url: {e}")

        if len(uploads) > 0:
            try:
                rag = get_rag_chain()
                for upload_meta in uploads:
                    uploaded_file_id = upload_meta.get("file_id")
                    uploaded_file_name = upload_meta.get("file_name", file_name)

                    report_meta = next((r for r in reports if r.get("file_name") == uploaded_file_name), None)
                    if report_meta:
                        processor = get_document_processor()
                        docs = await processor.load_document(report_meta.get("file_content"), uploaded_file_name)
                        if docs:
                            chunks = await processor.process_documents(docs)
                            for doc in chunks:
                                metadata = {
                                    "file_id": uploaded_file_id,
                                    "file_name": uploaded_file_name,
                                    "tenant_id": tenant_id,
                                    "storage_type": "drive",
                                    "source_type": "process_output",
                                    "activity_name": activity_name,
                                    "workitem_id": workitem_id,
                                }
                                if proc_inst_id:
                                    metadata["proc_inst_id"] = proc_inst_id
                                doc.metadata.update(metadata)

                            success = await rag.process_and_store_documents(chunks, tenant_id)
                            if success and uploaded_file_id:
                                await rag.save_processed_files([uploaded_file_id], tenant_id, [uploaded_file_name])

            except Exception as e:
                print(f"RAG processing after upload failed: {str(e)}")

        print("success process output")
        return {
            "message": "success process output",
            "uploaded": uploads,
            "folder_path": folder_path,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/save-to-storage")
async def save_to_storage(
    file: UploadFile = File(...),
    tenant_id: str = Form(...),
    options: Optional[str] = Form(None),
):
    """Supabase Storage 업로드 + 콘텐츠 추출/벡터스토어 저장."""
    try:
        proc_inst_id = None
        room_id = None
        raw_only = False
        if options:
            try:
                options_dict = json.loads(options)
                proc_inst_id = options_dict.get("proc_inst_id")
                room_id = options_dict.get("room_id")
                # 원본만 보관: 첨부를 워크스페이스의 실제 파일로 읽는 대화(Codex)는
                # 벡터 검색을 쓰지 않는다. 그쪽에는 VLM 판독과 임베딩이 순수 낭비이고,
                # 업로드가 느려지고 실패할 이유만 늘어난다.
                raw_only = bool(options_dict.get("raw_only"))
            except json.JSONDecodeError:
                pass

        file_content = await file.read()
        file_name = file.filename or "unknown"
        file_extension = Path(file_name).suffix.lower()

        # 채팅방 직접 첨부 정책 — 프론트 검증을 우회해도 서버에서 동일하게 차단한다.
        if room_id:
            # /process-session-file 의 _CHAT_ALLOWED_EXTS 및 프론트 채팅 첨부 목록과 같은 집합이어야
            # 한다. 여기만 좁으면 업로드가 400 이라 URL 이 안 생기고, 에이전트는 파일이 없는 것처럼 돈다.
            chat_allowed_extensions = {".pdf", ".hwp", ".hwpx", ".doc", ".docx", ".pptx", ".txt", ".xlsx"}
            chat_max_file_size = 10 * 1024 * 1024
            if file_extension not in chat_allowed_extensions:
                raise HTTPException(
                    status_code=400,
                    detail="지원하지 않는 파일 형식입니다. 허용: PDF, HWP, HWPX, DOC, DOCX, PPTX, TXT, XLSX",
                )
            if len(file_content) > chat_max_file_size:
                raise HTTPException(status_code=413, detail="파일은 10MB 이하만 업로드할 수 있습니다.")

        print(
            f"[ingest:save-to-storage] file={file_name!r} size={len(file_content)}B "
            f"tenant={tenant_id!r} proc_inst_id={proc_inst_id!r} room_id={room_id!r}"
        )

        storage_loader = SupabaseStorageLoader()
        upload_result = await storage_loader.upload_file_to_storage(
            file_content, file_name, folder_path="files"
        )
        storage_file_path = upload_result["file_path"]
        print(f"[ingest:save-to-storage] uploaded path={storage_file_path}")

        image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]
        is_image = file_extension in image_extensions
        # 벡터 인덱싱 제외 확장자 — 표 격자는 청킹·임베딩이 무의미하다(아래 분기 주석 참고).
        skip_vector_index = file_extension in {".xlsx", ".xlsm"} or raw_only

        has_uploaded_images = False
        # 비이미지 문서의 페이지 docs — 아래에서 document_pages 등록에 재사용(채팅 첨부도
        # KB 파일과 동일하게 read_document_page/grep 이 되도록).
        page_docs_for_reg = None

        if is_image:
            file_id = storage_file_path.replace("/", "_").replace("\\", "_")
            documents = await process_image_file(
                file_content,
                file_name,
                file_id,
                tenant_id,
                proc_inst_id,
                storage_type="storage",
                storage_file_path=storage_file_path,
                public_url=upload_result.get("public_url"),
            )
            if documents and room_id:
                for doc in documents:
                    doc.metadata["room_id"] = room_id
                    doc.metadata["knowledge_scope"] = "room"
            elif documents:
                for doc in documents:
                    doc.metadata["knowledge_scope"] = "global"
        elif raw_only:
            # 원본만 보관한다 — 이미지 추출·VLM 판독·청킹·임베딩을 모두 건너뛴다.
            # 이 대화의 에이전트는 업로드된 원본을 자기 워크스페이스에서 직접 열어
            # 읽으므로, 검색용 부산물은 만들어도 아무도 쓰지 않는다.
            documents = []
            print(f"[ingest:save-to-storage] raw_only → 파싱·임베딩 생략 (file={file_name!r})")
        else:
            file_io = io.BytesIO(file_content)
            processor = get_document_processor()

            file_id_for_images = storage_file_path.replace("/", "_").replace("\\", "_")
            uploaded_images = await processor.extract_and_upload_images_batched(
                file_content, file_name, file_id_for_images, tenant_id, batch_size=15,
            )
            has_uploaded_images = len(uploaded_images) > 0

            docs = await processor.load_document(file_io, file_name)
            if not docs:
                raise HTTPException(status_code=400, detail="Failed to load document")
            page_docs_for_reg = docs  # document_pages 등록용(페이지 단위 본문)

            if skip_vector_index:
                # 엑셀은 셀 격자라 의미 검색 대상이 아니다. 표를 800자로 잘라 임베딩하면 행이
                # 토막나 검색 품질만 나빠지고 비용만 든다. 텍스트(document_pages)만 남기면
                # grep/read_document_page 로 정확히 읽히고, 값 추출은 원본을 openpyxl 로 연다.
                documents = []
                print(f"[ingest:save-to-storage] {file_extension} → 청킹·임베딩 생략 (텍스트만 보존)")
            else:
                documents = await processor.process_documents(docs, {
                    "storage_type": "storage",
                    "file_path": storage_file_path,
                    "file_name": file_name,
                    "tenant_id": tenant_id,
                })

            for doc in documents:
                doc.metadata.update({
                    "file_id": storage_file_path,
                    "file_name": file_name,
                    "tenant_id": tenant_id,
                    "storage_type": "storage",
                })
                if proc_inst_id:
                    doc.metadata["proc_inst_id"] = proc_inst_id
                if room_id:
                    doc.metadata["room_id"] = room_id
                    doc.metadata["knowledge_scope"] = "room"
                else:
                    doc.metadata["knowledge_scope"] = "global"

        if not documents and not has_uploaded_images and not skip_vector_index:
            return {
                "message": "File uploaded to storage (no content extracted)",
                "file_path": storage_file_path,
                "file_name": file_name,
                "public_url": upload_result.get("public_url"),
                "processed": False,
            }

        rag = get_rag_chain()
        if documents:
            success = await rag.process_and_store_documents(documents, tenant_id)

            if not success:
                print(f"Vector store processing failed for {file_name}, but file is uploaded")
                return {
                    "message": "File uploaded to storage (vector processing failed)",
                    "file_path": storage_file_path,
                    "file_name": file_name,
                    "public_url": upload_result.get("public_url"),
                    "processed": False,
                }

        await rag.save_processed_files([storage_file_path], tenant_id, [file_name])

        # ── 채팅 첨부를 KB 파일과 *동일하게* 만드는 등록 ──
        # knowledge_files(카탈로그/경로해석) + document_pages(페이지 본문)를 채워, 프론트가 이
        # file_id(=storage_file_path)를 에이전트에 넘기면 에이전트가 재-ingest 없이 KB 선택 파일과
        # 똑같이 읽는다(search/catalog/read_document_page/grep). folder_path="" 로 KB 브라우저엔 숨김.
        # 벡터는 위에서 이미 room 스코프로 저장돼 room-RAG 도 그대로 동작.
        try:
            from app.services.knowledge_files import register_uploaded_file, INDEX_STATUS_INDEXED
            await register_uploaded_file(
                tenant_id=tenant_id,
                storage_path=storage_file_path,
                file_name=file_name,
                folder_path="",
                initial_status=INDEX_STATUS_INDEXED,
                doc_role="content",
            )
        except Exception as exc:  # noqa: BLE001 — 등록 실패가 업로드를 막지 않게
            print(f"[save-to-storage] knowledge_files 등록 실패 (계속): {exc}")

        if page_docs_for_reg:
            try:
                from app.services.document_pages import post_load_hook
                await post_load_hook(tenant_id, storage_file_path, page_docs_for_reg, skip_abstract=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[save-to-storage] document_pages 저장 실패 (계속): {exc}")

        return {
            "message": "File uploaded, processed, and stored successfully",
            "file_id": storage_file_path,   # ← 프론트가 이걸 에이전트 payload 로 넘기면 재-ingest 스킵
            "file_path": storage_file_path,
            "file_name": file_name,
            "public_url": upload_result.get("public_url"),
            "processed": True,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
