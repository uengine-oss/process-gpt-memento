from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2AuthorizationCodeBearer
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import os
import io
import asyncio
from supabase import create_client, Client
from datetime import datetime, timedelta
from urllib.parse import urlencode
import json
from fastapi.responses import JSONResponse
import httpx

from document_loader import DocumentProcessor
from google_drive_loader import GoogleDriveLoader
from supabase_storage_loader import SupabaseStorageLoader
from rag_chain import RAGChain
from image_storage_utils import ImageStorageUtils

from markdown_converter import convert_markdown_to_docx
from form2docx_converter import form_to_docx
from langchain.schema import Document
from pathlib import Path
from googleapiclient.http import MediaIoBaseDownload
import uuid


app = FastAPI(title="Memento Service API", description="API for document processing and querying")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl="https://accounts.google.com/o/oauth2/auth",
    tokenUrl="https://oauth2.googleapis.com/token"
)

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# Drive folder indexing job state (in-memory)
drive_jobs: Dict[str, dict] = {}
tenant_active_job: Dict[str, str] = {}

class ProcessRequest(BaseModel):
    storage_type: str = "drive"
    tenant_id: str
    input_dir: Optional[str] = None
    folder_path: Optional[str] = None
    file_path: Optional[str] = None
    original_filename: Optional[str] = None
    options: Optional[dict] = None

class RetrieveRequest(BaseModel):
    query: str
    tenant_id: str
    proc_inst_id: Optional[str] = None
    options: Optional[dict] = None

class GoogleOAuthRequest(BaseModel):
    tenant_id: str

class GoogleTokenRequest(BaseModel):
    tenant_id: str
    access_token: str
    refresh_token: Optional[str] = None
    expires_in: Optional[int] = None
    token_type: str = "Bearer"
    scopes: Optional[list[str]] = None

class GoogleOAuthCallbackRequest(BaseModel):
    code: str
    state: str  # This should be the tenant_id
    scope: Optional[str] = None

class ProcessOutputRequest(BaseModel):
    workitem_id: str
    tenant_id: Optional[str] = None

class UploadRequest(BaseModel):
    tenant_id: str
    options: Optional[dict] = None

async def process_image_file(file_content: bytes, file_name: str, file_id: str, tenant_id: str, proc_inst_id: Optional[str] = None, storage_type: str = 'storage', storage_file_path: Optional[str] = None, public_url: Optional[str] = None) -> Optional[List[Document]]:
    """이미지 파일을 처리하여 Document 객체로 변환"""
    try:
        from image_storage_utils import ImageStorageUtils
        
        # 파일 확장자 확인
        file_extension = Path(file_name).suffix.lower()
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
        
        if file_extension not in image_extensions:
            print(f"Unsupported image file type: {file_extension}")
            return None
        
        # 이미 저장된 파일 경로가 있으면 재사용, 없으면 새로 업로드
        if storage_file_path and public_url:
            # 이미 저장된 파일을 사용
            image_url = public_url
        else:
            # Supabase Storage에 이미지 업로드 (단일 이미지 업로드)
            storage_utils = ImageStorageUtils()
            upload_result = await storage_utils.upload_image_to_storage(
                file_content,
                file_name
            )
            
            if not upload_result:
                print(f"Failed to upload image {file_name}")
                return None
            
            image_url = upload_result.get('public_url')
        
        # Document 객체 생성
        metadata = {
            'file_id': file_id,
            'file_name': file_name,
            'tenant_id': tenant_id,
            'storage_type': storage_type,
            'image_count': 1,
            'source': file_name,
            'file_type': file_extension[1:],
            'image_url': image_url
        }
        
        # 인스턴스 아이디가 있으면 메타데이터에 추가
        if proc_inst_id:
            metadata['proc_inst_id'] = proc_inst_id
        
        doc = Document(
            page_content="",  # 이미지 분석 후 내용이 추가됨
            metadata=metadata
        )
        
        return [doc]
        
    except Exception as e:
        print(f"Error processing image file {file_name}: {e}")
        return None

@app.get("/auth/google/url")
async def get_google_auth_url(tenant_id: str):
    """Get Google OAuth authorization URL for the tenant"""
    try:
        # Get OAuth settings from database
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()

        
        if not response.data:
            raise HTTPException(status_code=404, detail="OAuth settings not found for tenant")
        
        oauth_settings = response.data
        
        # Create authorization URL
        params = {
            'client_id': oauth_settings['client_id'],
            'redirect_uri': oauth_settings['redirect_uri'],
            'scope': ' '.join([
                'openid',
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/drive.readonly',
                'https://www.googleapis.com/auth/drive.file'
            ]),
            'response_type': 'code',
            'access_type': 'offline',
            'prompt': 'consent',
            'state': tenant_id
        }
        
        auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
        
        return {
            "auth_url": auth_url,
            "state": tenant_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/auth/google/status")
async def get_google_auth_status(tenant_id: str):
    """Check if tenant has valid Google OAuth tokens"""
    try:
        response = supabase.table("tenant_oauth") \
            .select("google_credentials, google_credentials_updated_at") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()
        
        if not response.data or not response.data.get('google_credentials'):
            return {"authenticated": False, "message": "No Google credentials found"}
        
        # Parse token data
        if type(response.data['google_credentials']) == str:
            token_data = json.loads(response.data['google_credentials'])
        else:
            token_data = response.data['google_credentials']
        
        # Check if token is expired
        if token_data.get('expiry'):
            expiry = datetime.fromisoformat(token_data['expiry'])
            if datetime.utcnow() > expiry:
                return {"authenticated": False, "message": "Token expired"}
        
        return {
            "authenticated": True,
            "tenant_id": tenant_id,
            "expires_at": token_data.get('expiry'),
            "updated_at": response.data.get('google_credentials_updated_at')
        }
        
    except Exception as e:
        return {"authenticated": False, "message": str(e)}

@app.get("/retrieve")
async def retrieve(query: str, tenant_id: str, proc_inst_id: Optional[str] = None):
    try:
        if proc_inst_id:
            metadata_filter = {
                "tenant_id": tenant_id,
                "proc_inst_id": proc_inst_id
            }
        else:
            metadata_filter = {
                "tenant_id": tenant_id,
                "source_type": "process_output"
            }

        rag = RAGChain()

        result = await rag.retrieve(query, metadata_filter)
        docs = result["source_documents"]

        return {
            "response": docs,
            "metadata": {
                f"{doc.metadata.get('file_name', 'unknown')}#{doc.metadata.get('chunk_index', i)}": doc.metadata
                for i, doc in enumerate(docs)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-output")
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
        
        # Build Drive folder path: {proc_def_id}/{year}/{month}/{day}/{proc_inst_id}/output/
        proc_def_id = workitem_data.get("proc_def_id")
        proc_inst_id = workitem_data.get("proc_inst_id")
        # Use today's date
        today = datetime.now()
        year = f"{today.year:04d}"
        month = f"{today.month:02d}"
        day = f"{today.day:02d}"
        folder_path = f"{proc_def_id}/{year}/{month}/{day}/{proc_inst_id}/output/"

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
                        'file_content': io.BytesIO(docx_bytes),
                        'file_name': file_name
                    })

        if len(reports) > 0:
            for report in reports:
                file_name = report.get('file_name')
                file_content = report.get('file_content')
                upload_meta = await drive_loader.save_to_google_drive(
                    file_content=file_content,
                    file_name=file_name,
                    folder_path=folder_path
                )
                uploads.append(upload_meta)
        else:
            file_name = f"{activity_name}.docx"
            docx_bytes = form_to_docx(form_html, output)
            reports.append({
                'file_content': io.BytesIO(docx_bytes),
                'file_name': file_name
            })
            upload_meta = await drive_loader.save_to_google_drive(
                file_content=io.BytesIO(docx_bytes),
                file_name=file_name,
                folder_path=folder_path
            )
            uploads.append(upload_meta)
            try:
                output_url = upload_meta.get('web_view_link')
                supabase.table("todolist").update({"output_url": output_url}).eq("id", workitem_id).execute()
            except Exception as e:
                print(f"Error saving output url: {e}")

        if len(uploads) > 0:
            # Process generated DOCX bytes directly into vector store
            try:
                rag = RAGChain()
                for upload_meta in uploads:
                    uploaded_file_id = upload_meta.get('file_id')
                    uploaded_file_name = upload_meta.get('file_name', file_name)
                    
                    report_meta = next((report for report in reports if report.get('file_name') == uploaded_file_name), None)
                    if report_meta:
                        processor = DocumentProcessor()
                        docs = await processor.load_document(report_meta.get('file_content'), uploaded_file_name)
                        if docs:
                            chunks = await processor.process_documents(docs)
                            for doc in chunks:
                                metadata = {
                                    'file_id': uploaded_file_id,
                                    'file_name': uploaded_file_name,
                                    'tenant_id': tenant_id,
                                    'storage_type': 'drive',
                                    'source_type': 'process_output',
                                    'activity_name': activity_name,
                                    'workitem_id': workitem_id
                                }
                                if proc_inst_id:
                                    metadata['proc_inst_id'] = proc_inst_id
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
            "folder_path": folder_path
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process")
async def process(request: ProcessRequest):
    if request.storage_type == "local":
        return await process_documents(request)
    elif request.storage_type == "drive":
        return await process_google_drive(request)
    elif request.storage_type == "storage":
        return await process_supabase_storage(request)

@app.get("/process/drive/status")
async def get_drive_indexing_status(tenant_id: str):
    """Get Drive folder indexing job status for polling."""
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

@app.post("/process/database")
async def process_database(request: ProcessRequest):
    return await process_database_records(request)

async def process_documents(request: ProcessRequest):
    """Process documents from the input directory"""
    if not os.path.exists(request.input_dir):
        raise HTTPException(status_code=404, detail=f"Directory {request.input_dir} does not exist")
    
    try:
        processor = DocumentProcessor()
        all_documents = []
        
        # Extract proc_inst_id from options if provided
        proc_inst_id = None
        if request.options and request.options.get("proc_inst_id"):
            proc_inst_id = request.options.get("proc_inst_id")
        
        # Image extensions
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
        
        # Process all files in directory
        for root, _, files in os.walk(request.input_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_extension = Path(file).suffix.lower()
                
                if file_extension in image_extensions:
                    # Process image file
                    with open(file_path, 'rb') as f:
                        file_content = f.read()
                    
                    # Use file path as file_id (sanitized)
                    file_id = file_path.replace(os.sep, '_').replace('/', '_').replace('\\', '_')
                    
                    image_docs = await process_image_file(file_content, file, file_id, request.tenant_id, proc_inst_id, storage_type='local')
                    if image_docs:
                        all_documents.extend(image_docs)
                else:
                    # Process document file
                    with open(file_path, 'rb') as f:
                        file_content = f.read()
                    
                    file_io = io.BytesIO(file_content)
                    docs = await processor.load_document(file_io, file_path)
                    if docs:
                        chunks = await processor.process_documents(docs, {"storage_type": request.storage_type})
                        all_documents.extend(chunks)

        if not all_documents:
            return {"message": "No documents found to process"}
        
        # Update metadata for each document
        for doc in all_documents:
            if proc_inst_id:
                doc.metadata['proc_inst_id'] = proc_inst_id

        rag = RAGChain()
        success = await rag.process_and_store_documents(all_documents, request.tenant_id)
        
        if success:
            return {"message": "Successfully processed and stored documents"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process and store documents")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

SUPPORTED_MIME_TYPES = [
    'application/vnd.google-apps.document',
    'application/vnd.google-apps.spreadsheet',
    'application/vnd.google-apps.presentation',
    'application/pdf',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/x-hwp',
    'application/haansofthwp',
    'application/vnd.hancom.hwp',
    'application/vnd.hancom.hwpx',
    'text/plain',
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/bmp',
    'image/webp'
]

IMAGE_MIME_TYPES = [
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/bmp',
    'image/webp'
]

async def _run_drive_folder_async(
    job_id: str,
    tenant_id: str,
    new_files: List[dict],
    proc_inst_id: Optional[str] = None
) -> None:
    """Background task: process unprocessed files in Drive folder, update drive_jobs."""
    drive_jobs[job_id] = {
        "tenant_id": tenant_id,
        "status": "running",
        "total": len(new_files),
        "processed": 0,
        "failed": 0,
        "results": [],
        "error": None,
        "created_at": datetime.now().isoformat()
    }
    tenant_active_job[tenant_id] = job_id

    try:
        drive_loader = GoogleDriveLoader(tenant_id=tenant_id)
        await drive_loader.authenticate()
        rag = RAGChain()
        successful_file_ids = []
        successful_file_names = []
        all_documents = []

        for file in new_files:
            try:
                file_mime_type = file.get('mimeType', '')
                is_image = file_mime_type in IMAGE_MIME_TYPES

                if is_image:
                    request_media = drive_loader.service.files().get_media(fileId=file['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request_media)
                    done = False
                    while not done:
                        status, done = await asyncio.to_thread(downloader.next_chunk)
                    fh.seek(0)
                    file_content = fh.read()
                    documents = await process_image_file(
                        file_content, file['name'], file['id'], tenant_id, proc_inst_id, storage_type='drive'
                    )
                else:
                    documents = await drive_loader.download_and_process_file(
                        file['id'], file['name'], tenant_id
                    )

                if documents:
                    for doc in documents:
                        metadata = {
                            'file_id': file['id'],
                            'file_name': file['name'],
                            'tenant_id': tenant_id,
                            'storage_type': 'drive'
                        }
                        if proc_inst_id:
                            metadata['proc_inst_id'] = proc_inst_id
                        doc.metadata.update(metadata)
                    all_documents.extend(documents)
                    successful_file_ids.append(file['id'])
                    successful_file_names.append(file['name'])
                    drive_jobs[job_id]["results"].append({
                        "file_id": file['id'],
                        "file_name": file['name'],
                        "success": True
                    })
                    drive_jobs[job_id]["processed"] += 1
                else:
                    drive_jobs[job_id]["results"].append({
                        "file_id": file['id'],
                        "file_name": file['name'],
                        "success": False,
                        "error": "No content extracted"
                    })
                    drive_jobs[job_id]["failed"] += 1
            except Exception as e:
                drive_jobs[job_id]["results"].append({
                    "file_id": file['id'],
                    "file_name": file.get('name', 'unknown'),
                    "success": False,
                    "error": str(e)
                })
                drive_jobs[job_id]["failed"] += 1
                print(f"Error processing file {file.get('name', 'unknown')}: {e}")

        if all_documents:
            success = await rag.process_and_store_documents(all_documents, tenant_id)
            if success and successful_file_ids:
                await rag.save_processed_files(
                    successful_file_ids,
                    tenant_id,
                    successful_file_names
                )

        drive_jobs[job_id]["status"] = "completed"
    except Exception as e:
        drive_jobs[job_id]["status"] = "failed"
        drive_jobs[job_id]["error"] = str(e)
        print(f"Drive folder indexing failed: {e}")
    finally:
        tenant_active_job.pop(tenant_id, None)

async def process_google_drive(request: ProcessRequest):
    """Process documents from Google Drive folder"""
    try:
        drive_loader = GoogleDriveLoader(tenant_id=request.tenant_id)
        proc_inst_id = request.options.get("proc_inst_id") if request.options else None

        # Check if specific file_path is provided
        if hasattr(request, 'file_path') and request.file_path:
            # Process only the specified file (synchronous)
            try:
                file_id = request.file_path
                file_info = await drive_loader.get_file_info(file_id)
                if not file_info:
                    raise HTTPException(status_code=404, detail=f"File with ID {file_id} not found")
                
                rag = RAGChain()
                processed_files = await rag.get_processed_files(request.tenant_id)
                
                if file_id in processed_files:
                    return {"message": f"File {file_id} has already been processed for tenant {request.tenant_id}"}
                
                file_mime_type = file_info.get('mimeType', '')
                is_image = file_mime_type in IMAGE_MIME_TYPES
                
                if is_image:
                    # Process image file
                    request_media = drive_loader.service.files().get_media(fileId=file_id)
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request_media)
                    done = False
                    
                    while not done:
                        status, done = await asyncio.to_thread(downloader.next_chunk)
                    
                    fh.seek(0)
                    file_content = fh.read()
                    
                    documents = await process_image_file(file_content, file_info['name'], file_id, request.tenant_id, proc_inst_id, storage_type='drive')
                    if not documents:
                        return {"message": f"No content extracted from image file {file_id}"}
                else:
                    # Process document file
                    documents = await drive_loader.download_and_process_file(file_id, file_info['name'], request.tenant_id)
                    if not documents:
                        return {"message": f"No content extracted from file {file_id}"}
                
                # Update metadata for each document
                for doc in documents:
                    metadata = {
                        'file_id': file_id,
                        'file_name': file_info['name'],
                        'tenant_id': request.tenant_id,
                        'storage_type': 'drive'
                    }
                    if proc_inst_id:
                        metadata['proc_inst_id'] = proc_inst_id
                    doc.metadata.update(metadata)
                
                # Process and store the document
                success = await rag.process_and_store_documents(documents, request.tenant_id)
                if success:
                    # Save the processed file
                    await rag.save_processed_files([file_id], request.tenant_id, [file_info['name']])
                    return {"message": f"Successfully processed file {file_id}"}
                else:
                    raise HTTPException(status_code=500, detail="Failed to process and store document")
                    
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error processing file {file_id}: {str(e)}")
        
        else:
            # Async folder indexing: process all unprocessed files in background
            try:
                files = await drive_loader.list_files(SUPPORTED_MIME_TYPES)
            except ValueError as e:
                if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                    auth_response = create_auth_error_response(
                        request.tenant_id,
                        "Google Drive authentication required to process documents"
                    )
                    return JSONResponse(status_code=401, content=auth_response)
                raise e

            if not files:
                return {"message": f"No documents found in Google Drive folder for tenant {request.tenant_id}"}

            rag = RAGChain()
            processed_files = await rag.get_processed_files(request.tenant_id)
            new_files = [f for f in files if f['id'] not in processed_files]

            if not new_files:
                return {"message": f"No new documents to process for tenant {request.tenant_id}"}

            job_id = str(uuid.uuid4())
            asyncio.create_task(_run_drive_folder_async(
                job_id=job_id,
                tenant_id=request.tenant_id,
                new_files=new_files,
                proc_inst_id=proc_inst_id
            ))
            return JSONResponse(
                status_code=202,
                content={"job_id": job_id, "message": "인덱싱이 시작되었습니다"}
            )
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_supabase_storage(request: ProcessRequest):
    """Process a single file from Supabase Storage"""

    try:
        if not request.file_path:
            raise HTTPException(status_code=400, detail="file_path is required")
        
        original_filename = request.original_filename        
        if not original_filename:
            original_filename = os.path.basename(request.file_path)
        
        # Extract proc_inst_id from options if provided
        proc_inst_id = None
        if request.options and request.options.get("proc_inst_id"):
            proc_inst_id = request.options.get("proc_inst_id")
        
        # Check if file is an image
        file_extension = Path(original_filename).suffix.lower()
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
        is_image = file_extension in image_extensions
        
        storage_loader = SupabaseStorageLoader()
        
        if is_image:
            # Download image file
            response = await asyncio.to_thread(
                storage_loader.supabase.storage.from_("files").download,
                request.file_path
            )
            file_content = response if isinstance(response, bytes) else response.read() if hasattr(response, 'read') else bytes(response)
            
            # Use file_path as file_id (or generate one)
            file_id = request.file_path.replace('/', '_').replace('\\', '_')
            
            # Process image file
            documents = await process_image_file(file_content, original_filename, file_id, request.tenant_id, proc_inst_id, storage_type='storage')
        else:
            # Process document file
            documents = await storage_loader.download_and_process_file(
                request.file_path,
                metadata={
                    "tenant_id": request.tenant_id,
                    "original_filename": original_filename
                },
                tenant_id=request.tenant_id
            )
        
        if not documents:
            return {"message": "No content found in the file"}
        
        # Update metadata for each document
        for doc in documents:
            if proc_inst_id:
                doc.metadata['proc_inst_id'] = proc_inst_id
        
        rag = RAGChain()
        success = await rag.process_and_store_documents(documents, request.tenant_id)
        
        if success:
            return {"message": "Successfully processed and stored the file"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process and store the file")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_database_records(request: ProcessRequest):
    """Process documents from the database"""
    try:
        if not request.options:
            raise HTTPException(status_code=400, detail="options is required")
        
        supabase_storage_loader = SupabaseStorageLoader()
        supabase = supabase_storage_loader.supabase
        
        if supabase is None:
            raise HTTPException(status_code=500, detail="Failed to get Supabase client")
        
        query = supabase.table('todolist').select('*')
        for key, value in request.options.items():
            query = query.eq(key, value)
        
        result = query.execute()
        
        if result.data is None:
            raise HTTPException(status_code=404, detail="No documents found in the database")

        rag = RAGChain()
        tenant_id = request.tenant_id
        success = await rag.process_database_records(result.data, tenant_id, request.options)
        
        if success:
            return {"message": "Successfully processed and stored documents"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process and store documents")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query")
async def answer_query(
    query: str,
    tenant_id: str
):
    """Answer a query using the RAG system"""
    try:
        rag = RAGChain()
        
        metadata_filter = {
            "tenant_id": tenant_id
        }

        result = await rag.answer(query, metadata_filter)

        return {
            "response": result["answer"],
            "metadata": {
                f"{doc.metadata.get('file_name', 'unknown')}#{doc.metadata.get('chunk_index', i)}": doc.metadata
                for i, doc in enumerate(result["source_documents"])
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/save-to-storage")
async def save_to_storage(
    file: UploadFile = File(...),
    tenant_id: str = Form(...),
    options: Optional[str] = Form(None)  # JSON string
):
    """
    Upload a file to Supabase Storage and process it
    
    Flow:
    1. Upload file to Supabase Storage
    2. Process the file (extract content, images, etc.)
    3. Store in vector database
    
    Args:
        file: The file to upload
        tenant_id: Tenant ID
        options: Optional JSON string with additional options (e.g., {"proc_inst_id": "..."})
    
    Returns:
        Dictionary with file_path, file_name, public_url, and processed status
    """
    try:
        import json
        
        # Parse options if provided
        proc_inst_id = None
        if options:
            try:
                options_dict = json.loads(options)
                proc_inst_id = options_dict.get("proc_inst_id")
            except json.JSONDecodeError:
                pass
        
        # Read file content
        file_content = await file.read()
        file_name = file.filename or "unknown"
        
        # Upload to Supabase Storage
        storage_loader = SupabaseStorageLoader()
        upload_result = await storage_loader.upload_file_to_storage(
            file_content,
            file_name,
            folder_path="files"
        )
        
        storage_file_path = upload_result['file_path']
        
        # Check if file is an image
        file_extension = Path(file_name).suffix.lower()
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
        is_image = file_extension in image_extensions
        
        # Track if images were extracted and uploaded (for image-only PDFs)
        has_uploaded_images = False
        
        # Process the file
        if is_image:
            # Process image file
            # 이미 files 폴더에 저장했으므로, process_image_file에서는 다시 저장하지 않고 기존 경로 사용
            file_id = storage_file_path.replace('/', '_').replace('\\', '_')
            documents = await process_image_file(
                file_content, 
                file_name, 
                file_id, 
                tenant_id, 
                proc_inst_id,
                storage_type='storage',
                storage_file_path=storage_file_path,
                public_url=upload_result.get('public_url')
            )
        else:
            # Process document file
            file_io = io.BytesIO(file_content)
            processor = DocumentProcessor()
            
            # Extract images from document if supported
            file_id_for_images = storage_file_path.replace('/', '_').replace('\\', '_')
            extracted_images = await processor.extract_images_from_document(
                file_content,
                file_name,
                file_id_for_images
            )
            
            # Upload extracted images to storage
            if extracted_images:
                from image_storage_utils import ImageStorageUtils
                storage_utils = ImageStorageUtils()
                uploaded_images = await storage_utils.upload_images_batch(
                    extracted_images,
                    tenant_id,
                    file_id_for_images
                )
                # Store image URLs in metadata for later use
                image_urls = [img.get('image_url') for img in uploaded_images if img.get('image_url')]
                has_uploaded_images = len(uploaded_images) > 0
            
            docs = await processor.load_document(file_io, file_name)
            
            if not docs:
                raise HTTPException(status_code=400, detail="Failed to load document")
            
            # Process documents
            documents = await processor.process_documents(docs, {
                "storage_type": "storage",
                "file_path": storage_file_path,
                "file_name": file_name,
                "tenant_id": tenant_id
            })
            
            # Update metadata
            for doc in documents:
                doc.metadata.update({
                    'file_id': storage_file_path,
                    'file_name': file_name,
                    'tenant_id': tenant_id,
                    'storage_type': 'storage'
                })
                if proc_inst_id:
                    doc.metadata['proc_inst_id'] = proc_inst_id
        
        # Only raise exception if no documents AND no images were extracted
        # If images were extracted and uploaded, it's valid even if documents is empty
        if not documents and not has_uploaded_images:
            raise HTTPException(status_code=400, detail="No content extracted from file")
        
        # Store in vector database
        rag = RAGChain()
        success = await rag.process_and_store_documents(documents, tenant_id)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to process and store documents")
        
        # Save processed file info
        await rag.save_processed_files([storage_file_path], tenant_id, [file_name])
        
        return {
            "message": "File uploaded, processed, and stored successfully",
            "file_path": storage_file_path,
            "file_name": file_name,
            "public_url": upload_result.get('public_url'),
            "processed": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/save-to-drive")
async def save_to_drive(
    file: UploadFile = File(...),
    file_name: str = Form(...),
    tenant_id: str = Form(...),
    folder_path: Optional[str] = Form(None)
):
    """
    Save a file to Google Drive
    
    Args:
        file: The file to upload
        file_name: Name of the file
        tenant_id: Tenant ID for authentication
        folder_path: Optional folder path like "프로세스 정의/년/월/일/인스턴스/source/" (creates folders if they don't exist)
    """
    try:
        content = await file.read()
        
        # Create a BytesIO object from the content
        file_content = io.BytesIO(content)
        
        # Use tenant-level authentication
        drive_loader = GoogleDriveLoader(tenant_id=tenant_id)
        
        try:
            result = await drive_loader.save_to_google_drive(file_content, file_name, folder_path=folder_path)
            return result
        except ValueError as e:
            # Authentication error - return login URL
            if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                auth_response = create_auth_error_response(
                    tenant_id,
                    "Google Drive authentication required to upload files"
                )
                return JSONResponse(
                    status_code=401,
                    content=auth_response
                )
            else:
                raise e
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/google/save-token")
async def save_google_token(request: GoogleTokenRequest):
    """Save Google OAuth token to tenant's google_credentials column"""
    try:
        # Check if tenant_oauth record exists
        tenant_check = supabase.table("tenant_oauth") \
            .select("tenant_id") \
            .eq("tenant_id", request.tenant_id) \
            .single() \
            .execute()
        
        if not tenant_check.data:
            raise HTTPException(status_code=404, detail=f"Tenant OAuth settings not found for tenant {request.tenant_id}")

        # Prepare token data as JSON
        token_data = {
            "access_token": request.access_token,
            "refresh_token": request.refresh_token,
            "token_type": request.token_type,
            "expires_in": request.expires_in,
            "scopes": request.scopes or [
                'https://www.googleapis.com/auth/drive.readonly',
                'https://www.googleapis.com/auth/drive.file'
            ]
        }
        
        # Add expiry timestamp if expires_in is provided
        if request.expires_in:
            from datetime import datetime, timedelta, timezone
            expiry = datetime.now(timezone.utc) + timedelta(seconds=request.expires_in)
            token_data["expiry"] = expiry.isoformat()
        
        # Update tenant's google_credentials
        response = supabase.table("tenant_oauth") \
            .update({
                "google_credentials": json.dumps(token_data),
                "google_credentials_updated_at": datetime.now(timezone.utc).isoformat()
            }) \
            .eq("tenant_id", request.tenant_id) \
            .execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="Failed to update tenant credentials")

        return {
            "message": "Google token saved successfully",
            "tenant_id": request.tenant_id
        }

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to save Google token: {str(e)}")

@app.post("/auth/google/callback")
async def google_oauth_callback(request: GoogleOAuthCallbackRequest):
    """Handle Google OAuth callback and exchange code for tokens"""
    try:
        tenant_id = request.state
        
        # Get OAuth settings from database
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="OAuth settings not found for tenant")
        
        oauth_settings = response.data

        # Exchange authorization code for access token
        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            'client_id': oauth_settings['client_id'],
            'client_secret': oauth_settings['client_secret'],
            'code': request.code,
            'grant_type': 'authorization_code',
            'redirect_uri': oauth_settings['redirect_uri']
        }

        async with httpx.AsyncClient() as client:
            token_response = await client.post(token_url, data=token_data)
            
            if token_response.status_code != 200:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Failed to exchange code for token: {token_response.text}"
                )
            
            token_info = token_response.json()
            
            # Validate token response
            if 'access_token' not in token_info:
                raise HTTPException(
                    status_code=400,
                    detail=f"Token response missing access_token: {token_info}"
                )
        
        # Save token using existing endpoint
        token_request = GoogleTokenRequest(
            tenant_id=tenant_id,
            access_token=token_info['access_token'],
            refresh_token=token_info.get('refresh_token'),
            expires_in=token_info.get('expires_in'),
            token_type=token_info.get('token_type', 'Bearer'),
            scopes=request.scope.split(' ') if request.scope else None
        )
        
        # Call the existing save token function
        await save_google_token(token_request)
        
        return {
            "message": "Google OAuth completed successfully",
            "tenant_id": tenant_id,
            "token_saved": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def create_auth_error_response(tenant_id: str, error_message: str = "Authentication required"):
    """Create a standardized auth error response with login URL"""
    try:
        # Get OAuth settings to create login URL
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()
        
        if response.data:
            oauth_settings = response.data
            
            # Create authorization URL
            params = {
                'client_id': oauth_settings['client_id'],
                'redirect_uri': oauth_settings['redirect_uri'],
                'scope': ' '.join([
                    'openid',
                    'https://www.googleapis.com/auth/userinfo.email',
                    'https://www.googleapis.com/auth/userinfo.profile',
                    'https://www.googleapis.com/auth/drive.readonly',
                    'https://www.googleapis.com/auth/drive.file'
                ]),
                'response_type': 'code',
                'access_type': 'offline',
                'prompt': 'consent',
                'state': tenant_id
            }
            
            auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
            
            return {
                "error": "authentication_required",
                "message": error_message,
                "auth_url": auth_url,
                "tenant_id": tenant_id
            }
        else:
            return {
                "error": "oauth_settings_not_found",
                "message": "OAuth settings not configured for this tenant",
                "tenant_id": tenant_id
            }
            
    except Exception as e:
        return {
            "error": "auth_url_generation_failed",
            "message": f"Failed to generate auth URL: {str(e)}",
            "tenant_id": tenant_id
        }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)


"""
- 문서 처리
http POST http://localhost:8005/process storage_type="drive"
http POST http://localhost:8005/process/database storage_type="database" options='{"proc_inst_id": "handover_process_definition.dae522ed-f93d-4f0c-b473-f1d79dbcf709", "activity_id": "plan_handover_schedule", "tenant_id": "localhost"}'

- 질의
http GET http://localhost:8005/query query=="프로젝트 A의 예산이 얼마 나왔지?" tenant_id==localhost

- 검색
http POST http://localhost:8005/retrieve query="교육" tenant_id="localhost"

- 산출물 처리
http POST http://localhost:8005/process-output workitem_id="bee83324-dc87-4f25-b7e2-e8e4ed5a3d8e" tenant_id="localhost"
"""
