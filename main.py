from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2AuthorizationCodeBearer
from pydantic import BaseModel
from typing import Optional, Dict, Any
import os
import io
import asyncio
from datetime import datetime, timedelta

from document_loader import DocumentProcessor
from google_drive_loader import GoogleDriveLoader
from supabase_storage_loader import SupabaseStorageLoader
from rag_chain import RAGChain
from auth import get_current_user, get_google_drive_service, handle_oauth_callback

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

class ProcessRequest(BaseModel):
    storage_type: str = "drive"
    input_dir: Optional[str] = None
    folder_path: Optional[str] = None
    file_path: Optional[str] = None
    original_filename: Optional[str] = None
    options: Optional[dict] = None

class RetrieveRequest(BaseModel):
    query: str
    options: Optional[dict] = None

@app.post("/retrieve")
async def retrieve(
    request: RetrieveRequest, 
):
    try:
        query_text = request.query

        if not query_text:
            raise HTTPException(status_code=400, detail="query is required")

        rag = RAGChain()
        
        metadata_filter = {
            "tenant_id": request.options.get('tenant_id')
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


@app.post("/process")
async def process(
    request: ProcessRequest,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    if request.storage_type == "local":
        return await process_documents(request, current_user)
    elif request.storage_type == "drive":
        return await process_google_drive(request, current_user)
    elif request.storage_type == "storage":
        return await process_supabase_storage(request, current_user)

@app.post("/process/database")
async def process_database(request: ProcessRequest):
    return await process_database_records(request)

async def process_documents(request: ProcessRequest, current_user: Dict[str, Any]):
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
        success = await rag.process_and_store_documents(documents, current_user.app_metadata['tenant_id'])
        
        if success:
            return {"message": "Successfully processed and stored documents"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process and store documents")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_google_drive(request: ProcessRequest, current_user: Dict[str, Any]):
    """Process documents from Google Drive folder"""
    try:
        drive_service = await get_google_drive_service(current_user)
        drive_loader = GoogleDriveLoader(drive_service)
        
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

        # Get list of files from Google Drive
        files = await drive_loader.list_files(supported_mime_types)
        
        if not files:
            return {"message": f"No documents found in Google Drive folder for user {current_user['email']}"}
            
        rag = RAGChain()
        
        # Get list of already processed files for this user
        processed_files = await rag.get_processed_files(current_user.app_metadata['tenant_id'])
        
        # Filter out already processed files
        new_files = [f for f in files if f['id'] not in processed_files]
        
        if not new_files:
            return {"message": f"No new documents to process for user {current_user['email']}"}
            
        # Process only new files
        all_documents = []
        for file in new_files:
            documents = await drive_loader.download_and_process_file(file['id'], file['name'])
            if documents:
                # Update metadata for each document
                for doc in documents:
                    doc.metadata.update({
                        'file_id': file['id'],
                        'file_name': file['name'],
                        'tenant_id': current_user.app_metadata['tenant_id'],
                        'user_id': current_user.id,
                        'storage_type': 'drive'
                    })
                all_documents.extend(documents)
        
        if all_documents:
            success = await rag.process_and_store_documents(all_documents, current_user.app_metadata['tenant_id'])
            if success:
                # Save the list of processed files
                await rag.save_processed_files(
                    [f['id'] for f in new_files],
                    current_user.app_metadata['tenant_id'],
                    [f['name'] for f in new_files]
                )
                return {"message": f"Successfully processed {len(new_files)} new documents"}
            else:
                raise HTTPException(status_code=500, detail="Failed to process and store documents")
        else:
            return {"message": f"No new content to process for user {current_user['email']}"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_supabase_storage(request: ProcessRequest, current_user: Dict[str, Any]):
    """Process a single file from Supabase Storage"""
    print(f"Processing file from Supabase Storage...")
    
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
                "tenant_id": current_user.app_metadata['tenant_id'],
                "user_id": current_user.id,
                "original_filename": original_filename
            }
        )
        
        if not documents:
            return {"message": "No content found in the file"}
        
        rag = RAGChain()
        success = await rag.process_and_store_documents(documents, current_user.app_metadata['tenant_id'])
        
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
        tenant_id = request.options.get('tenant_id')
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
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Answer a query using the RAG system"""
    try:
        rag = RAGChain()
        
        metadata_filter = {
            "tenant_id": current_user.app_metadata['tenant_id']
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
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Save a file to Google Drive
    
    Args:
        file: The file to upload
        file_name: Name of the file
    """
    try:
        content = await file.read()
        
        # Create a BytesIO object from the content
        file_content = io.BytesIO(content)
        
        drive_service = await get_google_drive_service(current_user)
        drive_loader = GoogleDriveLoader(drive_service)
        result = await drive_loader.save_to_google_drive(file_content, file_name)
        return result
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/oauth/google")
async def oauth_google(
    code: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Handle OAuth callback and get Google Drive service"""
    try:
        drive_service = await handle_oauth_callback(code, current_user)
        return {"message": "Successfully authenticated with Google Drive"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)


"""
- 문서 처리
http POST http://localhost:8005/process storage_type="drive"
http POST http://localhost:8005/process/database storage_type="database" options='{"proc_inst_id": "handover_process_definition.dae522ed-f93d-4f0c-b473-f1d79dbcf709", "activity_id": "plan_handover_schedule", "tenant_id": "localhost"}'

- 질의
http GET http://localhost:8005/query?query="프로젝트 A의 예산이 얼마 나왔지?"
"""
