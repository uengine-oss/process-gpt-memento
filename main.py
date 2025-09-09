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

from markdown_converter import convert_markdown_to_docx
from form2docx_converter import form_to_docx


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

@app.post("/retrieve")
async def retrieve(request: RetrieveRequest):
    try:
        query_text = request.query

        if not query_text:
            raise HTTPException(status_code=400, detail="query is required")

        rag = RAGChain()
        
        metadata_filter = {
            "tenant_id": request.tenant_id
        }

        result = await rag.retrieve(query_text, metadata_filter)
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
                                doc.metadata.update({
                                    'file_id': uploaded_file_id,
                                    'file_name': uploaded_file_name,
                                    'tenant_id': tenant_id,
                                    'storage_type': 'drive',
                                    'source_type': 'process_output',
                                    'activity_name': activity_name,
                                    'workitem_id': workitem_id
                                })

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

@app.post("/process/database")
async def process_database(request: ProcessRequest):
    return await process_database_records(request)

async def process_documents(request: ProcessRequest):
    """Process documents from the input directory"""
    if not os.path.exists(request.input_dir):
        raise HTTPException(status_code=404, detail=f"Directory {request.input_dir} does not exist")
    
    try:
        processor = DocumentProcessor()
        documents = await asyncio.to_thread(
            processor.process_directory,
            request.input_dir,
            {"storage_type": request.storage_type}
        )

        if not documents:
            return {"message": "No documents found to process"}

        rag = RAGChain()
        success = await rag.process_and_store_documents(documents, request.tenant_id)
        
        if success:
            return {"message": "Successfully processed and stored documents"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process and store documents")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_google_drive(request: ProcessRequest):
    """Process documents from Google Drive folder"""
    try:
        # Use tenant-level authentication
        drive_loader = GoogleDriveLoader(tenant_id=request.tenant_id)
        
        supported_mime_types = [
            'application/vnd.google-apps.document',
            'application/vnd.google-apps.spreadsheet',
            'application/vnd.google-apps.presentation',
            'application/pdf',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'text/plain'
        ]

        # Check if specific file_path is provided
        if hasattr(request, 'file_path') and request.file_path:
            # Process only the specified file
            try:
                # Assuming file_path contains the file ID
                file_id = request.file_path
                # Get file info (you might need to implement this method)
                file_info = await drive_loader.get_file_info(file_id)
                if not file_info:
                    raise HTTPException(status_code=404, detail=f"File with ID {file_id} not found")
                
                # Check if file is already processed
                rag = RAGChain()
                processed_files = await rag.get_processed_files(request.tenant_id)
                
                if file_id in processed_files:
                    return {"message": f"File {file_id} has already been processed for tenant {request.tenant_id}"}
                
                # Process the specific file
                documents = await drive_loader.download_and_process_file(file_id, file_info['name'], request.tenant_id)
                if not documents:
                    return {"message": f"No content extracted from file {file_id}"}
                
                # Update metadata for each document
                for doc in documents:
                    doc.metadata.update({
                        'file_id': file_id,
                        'file_name': file_info['name'],
                        'tenant_id': request.tenant_id,
                        'storage_type': 'drive'
                    })
                
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
            # Original logic: process all new files
            try:
                # Get list of files from Google Drive
                files = await drive_loader.list_files(supported_mime_types)
            except ValueError as e:
                # Authentication error - return login URL
                if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                    auth_response = create_auth_error_response(
                        request.tenant_id,
                        "Google Drive authentication required to process documents"
                    )
                    return JSONResponse(
                        status_code=401,
                        content=auth_response
                    )
                else:
                    raise e
            
            if not files:
                return {"message": f"No documents found in Google Drive folder for tenant {request.tenant_id}"}
                
            rag = RAGChain()
            
            # Get list of already processed files for this tenant
            processed_files = await rag.get_processed_files(request.tenant_id)
            
            # Filter out already processed files
            new_files = [f for f in files if f['id'] not in processed_files]
            
            if not new_files:
                return {"message": f"No new documents to process for tenant {request.tenant_id}"}
                
            # Process only new files
            all_documents = []
            for file in new_files:
                try:
                    documents = await drive_loader.download_and_process_file(file['id'], file['name'], request.tenant_id)
                    if documents:
                        # Update metadata for each document
                        for doc in documents:
                            doc.metadata.update({
                                'file_id': file['id'],
                                'file_name': file['name'],
                                'tenant_id': request.tenant_id,
                                'storage_type': 'drive'
                            })
                        all_documents.extend(documents)
                except Exception as e:
                    # Skip files that can't be processed
                    continue
            
            if all_documents:
                success = await rag.process_and_store_documents(all_documents, request.tenant_id)
                if success:
                    # Save the list of processed files
                    await rag.save_processed_files(
                        [f['id'] for f in new_files],
                        request.tenant_id,
                        [f['name'] for f in new_files]
                    )
                    return {"message": f"Successfully processed {len(new_files)} new documents"}
                else:
                    raise HTTPException(status_code=500, detail="Failed to process and store documents")
            else:
                return {"message": f"No new content to process for tenant {request.tenant_id}"}
                
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
        
        storage_loader = SupabaseStorageLoader()
        
        # Process single file
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
