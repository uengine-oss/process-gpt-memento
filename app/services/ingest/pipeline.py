"""스토리지 타입별 인제스트 파이프라인 + Drive 폴더 백그라운드 잡."""
from __future__ import annotations

import asyncio
import io
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from googleapiclient.http import MediaIoBaseDownload

from app.core import config
from app.core.auth import create_auth_error_response
from app.core.supabase_client import supabase
from app.schemas import ProcessRequest
from app.services.document_processor import get_document_processor
from app.services.google_drive_loader import GoogleDriveLoader
from app.services.rag_chain import get_rag_chain
from app.services.ingest.image import process_image_file
from app.services.ingest.state import (
    IMAGE_MIME_TYPES,
    SUPPORTED_MIME_TYPES,
    cleanup_drive_jobs,
    drive_jobs,
    tenant_active_job,
)
from app.storage.supabase_loader import SupabaseStorageLoader


async def run_drive_folder_async(
    job_id: str,
    tenant_id: str,
    new_files: List[dict],
    proc_inst_id: Optional[str] = None,
) -> None:
    """Drive 폴더 인덱싱 백그라운드 태스크 — drive_jobs 상태를 갱신한다."""
    drive_jobs[job_id] = {
        "tenant_id": tenant_id,
        "status": "running",
        "total": len(new_files),
        "processed": 0,
        "failed": 0,
        "results": [],
        "error": None,
        "created_at": datetime.now().isoformat(),
    }
    tenant_active_job[tenant_id] = job_id

    try:
        drive_loader = GoogleDriveLoader(tenant_id=tenant_id)
        await drive_loader.authenticate()
        rag = get_rag_chain()
        successful_file_ids: List[str] = []
        successful_file_names: List[str] = []
        all_documents: List = []

        for file in new_files:
            try:
                file_mime_type = file.get("mimeType", "")
                is_image = file_mime_type in IMAGE_MIME_TYPES

                if is_image:
                    request_media = drive_loader.service.files().get_media(fileId=file["id"])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request_media)
                    done = False
                    while not done:
                        _, done = await asyncio.to_thread(downloader.next_chunk)
                    fh.seek(0)
                    file_content = fh.read()
                    documents = await process_image_file(
                        file_content, file["name"], file["id"], tenant_id, proc_inst_id, storage_type="drive"
                    )
                else:
                    documents = await drive_loader.download_and_process_file(
                        file["id"], file["name"], tenant_id
                    )

                if documents:
                    for doc in documents:
                        metadata = {
                            "file_id": file["id"],
                            "file_name": file["name"],
                            "tenant_id": tenant_id,
                            "storage_type": "drive",
                        }
                        if file.get("drive_folder_id"):
                            metadata["drive_folder_id"] = file["drive_folder_id"]
                        if file.get("drive_folder_name"):
                            metadata["drive_folder_name"] = file["drive_folder_name"]
                        if proc_inst_id:
                            metadata["proc_inst_id"] = proc_inst_id
                        doc.metadata.update(metadata)
                    all_documents.extend(documents)
                    successful_file_ids.append(file["id"])
                    successful_file_names.append(file["name"])
                    drive_jobs[job_id]["results"].append({
                        "file_id": file["id"],
                        "file_name": file["name"],
                        "success": True,
                    })
                    drive_jobs[job_id]["processed"] += 1
                else:
                    drive_jobs[job_id]["results"].append({
                        "file_id": file["id"],
                        "file_name": file["name"],
                        "success": False,
                        "error": "No content extracted",
                    })
                    drive_jobs[job_id]["failed"] += 1
            except Exception as e:
                drive_jobs[job_id]["results"].append({
                    "file_id": file["id"],
                    "file_name": file.get("name", "unknown"),
                    "success": False,
                    "error": str(e),
                })
                drive_jobs[job_id]["failed"] += 1
                print(f"Error processing file {file.get('name', 'unknown')}: {e}")

        if all_documents:
            success = await rag.process_and_store_documents(all_documents, tenant_id)
            if success and successful_file_ids:
                await rag.save_processed_files(
                    successful_file_ids, tenant_id, successful_file_names
                )

        drive_jobs[job_id]["status"] = "completed"
        drive_jobs[job_id]["finished_at"] = datetime.now().isoformat()
    except Exception as e:
        drive_jobs[job_id]["status"] = "failed"
        drive_jobs[job_id]["error"] = str(e)
        drive_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        print(f"Drive folder indexing failed: {e}")
    finally:
        tenant_active_job.pop(tenant_id, None)
        cleanup_drive_jobs()


async def process_local_documents(request: ProcessRequest):
    if not os.path.exists(request.input_dir):
        raise HTTPException(status_code=404, detail=f"Directory {request.input_dir} does not exist")

    try:
        processor = get_document_processor()
        all_documents: List = []

        proc_inst_id = None
        if request.options and request.options.get("proc_inst_id"):
            proc_inst_id = request.options.get("proc_inst_id")

        image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]

        for root, _, files in os.walk(request.input_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_extension = Path(file).suffix.lower()

                if file_extension in image_extensions:
                    with open(file_path, "rb") as f:
                        file_content = f.read()
                    file_id = file_path.replace(os.sep, "_").replace("/", "_").replace("\\", "_")
                    image_docs = await process_image_file(
                        file_content, file, file_id, request.tenant_id, proc_inst_id, storage_type="local"
                    )
                    if image_docs:
                        all_documents.extend(image_docs)
                else:
                    with open(file_path, "rb") as f:
                        file_content = f.read()
                    file_io = io.BytesIO(file_content)
                    docs = await processor.load_document(file_io, file_path)
                    if docs:
                        chunks = await processor.process_documents(docs, {"storage_type": request.storage_type})
                        all_documents.extend(chunks)

        if not all_documents:
            return {"message": "No documents found to process"}

        for doc in all_documents:
            if proc_inst_id:
                doc.metadata["proc_inst_id"] = proc_inst_id

        rag = get_rag_chain()
        success = await rag.process_and_store_documents(all_documents, request.tenant_id)
        if success:
            return {"message": "Successfully processed and stored documents"}
        raise HTTPException(status_code=500, detail="Failed to process and store documents")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def process_google_drive(request: ProcessRequest):
    try:
        drive_loader = GoogleDriveLoader(tenant_id=request.tenant_id)
        proc_inst_id = request.options.get("proc_inst_id") if request.options else None
        extra_drive_folder_id = config.memento_drive_folder_id().strip()

        if hasattr(request, "file_path") and request.file_path:
            try:
                file_id = request.file_path
                file_info = await drive_loader.get_file_info(file_id)
                if not file_info:
                    raise HTTPException(status_code=404, detail=f"File with ID {file_id} not found")

                rag = get_rag_chain()
                processed_files = await rag.get_processed_files(request.tenant_id)
                if file_id in processed_files:
                    return {"message": f"File {file_id} has already been processed for tenant {request.tenant_id}"}

                file_mime_type = file_info.get("mimeType", "")
                is_image = file_mime_type in IMAGE_MIME_TYPES

                if is_image:
                    request_media = drive_loader.service.files().get_media(fileId=file_id)
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request_media)
                    done = False
                    while not done:
                        _, done = await asyncio.to_thread(downloader.next_chunk)
                    fh.seek(0)
                    file_content = fh.read()
                    documents = await process_image_file(
                        file_content, file_info["name"], file_id, request.tenant_id, proc_inst_id, storage_type="drive"
                    )
                    if not documents:
                        return {"message": f"No content extracted from image file {file_id}"}
                else:
                    documents = await drive_loader.download_and_process_file(file_id, file_info["name"], request.tenant_id)
                    if not documents:
                        return {"message": f"No content extracted from file {file_id}"}

                for doc in documents:
                    metadata = {
                        "file_id": file_id,
                        "file_name": file_info["name"],
                        "tenant_id": request.tenant_id,
                        "storage_type": "drive",
                    }
                    if proc_inst_id:
                        metadata["proc_inst_id"] = proc_inst_id
                    doc.metadata.update(metadata)

                success = await rag.process_and_store_documents(documents, request.tenant_id)
                if success:
                    await rag.save_processed_files([file_id], request.tenant_id, [file_info["name"]])
                    return {"message": f"Successfully processed file {file_id}"}
                raise HTTPException(status_code=500, detail="Failed to process and store document")

            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error processing file {file_id}: {str(e)}")

        try:
            files = await drive_loader.list_files_recursive(SUPPORTED_MIME_TYPES)
            if extra_drive_folder_id:
                extra_files = await drive_loader.list_files_recursive(SUPPORTED_MIME_TYPES, extra_drive_folder_id)
                merged: Dict[str, dict] = {f["id"]: f for f in extra_files if isinstance(f, dict) and f.get("id")}
                for item in files:
                    if isinstance(item, dict) and item.get("id") and item["id"] not in merged:
                        merged[item["id"]] = item
                files = list(merged.values())
        except ValueError as e:
            if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                auth_response = create_auth_error_response(
                    supabase, request.tenant_id, "Google Drive authentication required to process documents"
                )
                return JSONResponse(status_code=401, content=auth_response)
            raise e

        if not files:
            return {"message": f"No documents found in Google Drive folder for tenant {request.tenant_id}"}

        rag = get_rag_chain()
        processed_files = await rag.get_processed_files(request.tenant_id)
        new_files = [f for f in files if f["id"] not in processed_files]

        if not new_files:
            return {"message": f"No new documents to process for tenant {request.tenant_id}"}

        job_id = str(uuid.uuid4())
        asyncio.create_task(run_drive_folder_async(
            job_id=job_id,
            tenant_id=request.tenant_id,
            new_files=new_files,
            proc_inst_id=proc_inst_id,
        ))
        return JSONResponse(
            status_code=202,
            content={"job_id": job_id, "message": "인덱싱이 시작되었습니다"},
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def process_supabase_storage(request: ProcessRequest):
    try:
        if not request.file_path:
            raise HTTPException(status_code=400, detail="file_path is required")

        original_filename = request.original_filename or os.path.basename(request.file_path)

        proc_inst_id = None
        if request.options and request.options.get("proc_inst_id"):
            proc_inst_id = request.options.get("proc_inst_id")

        file_extension = Path(original_filename).suffix.lower()
        image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]
        is_image = file_extension in image_extensions

        storage_loader = SupabaseStorageLoader()

        if is_image:
            response = await asyncio.to_thread(
                storage_loader.supabase.storage.from_("files").download,
                request.file_path,
            )
            file_content = response if isinstance(response, bytes) else response.read() if hasattr(response, "read") else bytes(response)
            file_id = request.file_path.replace("/", "_").replace("\\", "_")
            documents = await process_image_file(
                file_content, original_filename, file_id, request.tenant_id, proc_inst_id, storage_type="storage"
            )
        else:
            documents = await storage_loader.download_and_process_file(
                request.file_path,
                metadata={"tenant_id": request.tenant_id, "original_filename": original_filename},
                tenant_id=request.tenant_id,
            )

        if not documents:
            return {"message": "No content found in the file"}

        for doc in documents:
            if proc_inst_id:
                doc.metadata["proc_inst_id"] = proc_inst_id

        rag = get_rag_chain()
        success = await rag.process_and_store_documents(documents, request.tenant_id)
        if success:
            return {"message": "Successfully processed and stored the file"}
        raise HTTPException(status_code=500, detail="Failed to process and store the file")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def process_database_records(request: ProcessRequest):
    try:
        if not request.options:
            raise HTTPException(status_code=400, detail="options is required")

        supabase_storage_loader = SupabaseStorageLoader()
        sb = supabase_storage_loader.supabase
        if sb is None:
            raise HTTPException(status_code=500, detail="Failed to get Supabase client")

        query = sb.table("todolist").select("*")
        for key, value in request.options.items():
            query = query.eq(key, value)
        result = query.execute()

        if result.data is None:
            raise HTTPException(status_code=404, detail="No documents found in the database")

        rag = get_rag_chain()
        success = await rag.process_database_records(result.data, request.tenant_id, request.options)
        if success:
            return {"message": "Successfully processed and stored documents"}
        raise HTTPException(status_code=500, detail="Failed to process and store documents")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
