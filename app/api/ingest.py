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
from app.schemas import ProcessOutputRequest, ProcessRequest
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
        if options:
            try:
                options_dict = json.loads(options)
                proc_inst_id = options_dict.get("proc_inst_id")
                room_id = options_dict.get("room_id")
            except json.JSONDecodeError:
                pass

        file_content = await file.read()
        file_name = file.filename or "unknown"
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

        file_extension = Path(file_name).suffix.lower()
        image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]
        is_image = file_extension in image_extensions

        has_uploaded_images = False

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

        if not documents and not has_uploaded_images:
            return {
                "message": "File uploaded to storage (no content extracted)",
                "file_path": storage_file_path,
                "file_name": file_name,
                "public_url": upload_result.get("public_url"),
                "processed": False,
            }

        rag = get_rag_chain()
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

        return {
            "message": "File uploaded, processed, and stored successfully",
            "file_path": storage_file_path,
            "file_name": file_name,
            "public_url": upload_result.get("public_url"),
            "processed": True,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
