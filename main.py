from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
import io

from document_loader import DocumentProcessor
from google_drive_loader import GoogleDriveLoader
from supabase_storage_loader import SupabaseStorageLoader
from rag_chain import RAGChain

app = FastAPI(title="Memento Service API", description="API for document processing and querying")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ProcessRequest(BaseModel):
    storage_type: str = "drive"
    tenant_id: Optional[str] = "localhost"
    input_dir: Optional[str] = None
    folder_path: Optional[str] = None
    file_path: Optional[str] = None
    original_filename: Optional[str] = None

@app.post("/process")
async def process(request: ProcessRequest):
    if request.storage_type == "local":
        return process_documents(request)
    elif request.storage_type == "drive":
        return process_google_drive(request)
    elif request.storage_type == "storage":
        return process_supabase_storage(request)

def process_documents(request: ProcessRequest):
    """Process documents from the input directory"""
    if not os.path.exists(request.input_dir):
        raise HTTPException(status_code=404, detail=f"Directory {request.input_dir} does not exist")
    
    try:
        processor = DocumentProcessor()
        documents = processor.process_directory(request.input_dir, {"storage_type": request.storage_type})

        if not documents:
            return {"message": "No documents found to process"}
        
        if request.tenant_id is None:
            tenant_id = 'localhost'
        else:
            tenant_id = request.tenant_id

        rag = RAGChain()
        success = rag.process_and_store_documents(documents, tenant_id)
        
        if success:
            return {"message": "Successfully processed and stored documents"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process and store documents")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def process_google_drive(request: ProcessRequest):
    """Process documents from Google Drive folder"""
    try:
        drive_loader = GoogleDriveLoader()
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
        
        if request.tenant_id is None:
            tenant_id = 'localhost'
        else:
            tenant_id = request.tenant_id

        # Get list of files from Google Drive for specific tenant
        files = drive_loader.list_files(supported_mime_types, tenant_id=tenant_id)
        
        if not files:
            return {"message": f"No documents found in Google Drive folder for tenant {tenant_id}"}
            
        rag = RAGChain()
        
        # Get list of already processed files for this tenant
        processed_files = rag.get_processed_files(tenant_id)
        
        # Filter out already processed files
        new_files = [f for f in files if f['id'] not in processed_files]
        
        if not new_files:
            return {"message": f"No new documents to process for tenant {tenant_id}"}
            
        # Process only new files
        all_documents = []
        for file in new_files:
            documents = drive_loader.download_and_process_file(file['id'], file['name'])
            if documents:
                # Update metadata for each document
                for doc in documents:
                    doc.metadata.update({
                        'file_id': file['id'],
                        'file_name': file['name'],
                        'tenant_id': tenant_id,
                        'storage_type': 'drive'
                    })
                all_documents.extend(documents)
        
        if all_documents:
            success = rag.process_and_store_documents(all_documents, tenant_id)
            if success:
                # Save the list of processed files
                rag.save_processed_files(
                    [f['id'] for f in new_files],
                    tenant_id,
                    [f['name'] for f in new_files]
                )
                return {"message": f"Successfully processed {len(new_files)} new documents for tenant {tenant_id}"}
            else:
                raise HTTPException(status_code=500, detail="Failed to process and store documents")
        else:
            return {"message": f"No new content to process for tenant {tenant_id}"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def process_supabase_storage(request: ProcessRequest):
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
        documents = storage_loader.download_and_process_file(
            request.file_path,
            metadata={"tenant_id": request.tenant_id or "localhost", "original_filename": original_filename}
        )
        
        if not documents:
            return {"message": "No content found in the file"}
        
        rag = RAGChain()
        success = rag.process_and_store_documents(documents, request.tenant_id or "localhost")
        
        if success:
            return {"message": "Successfully processed and stored the file"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process and store the file")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query")
async def answer_query(
    query: str,
    tenant_id: Optional[str] = "localhost",
    storage_type: Optional[str] = None
):
    """Answer a query using the RAG system"""
    try:
        rag = RAGChain()
        
        if tenant_id is None:
            metadata_filter = {"tenant_id": "localhost"}
        else:
            metadata_filter = {"tenant_id": tenant_id}

        if storage_type:
            metadata_filter["storage_type"] = storage_type
            
        result = rag.answer(query, metadata_filter)

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
    tenant_id: str = Form(...)
):
    """
    Save a file to Google Drive
    
    Args:
        file: The file to upload
        file_name: Name of the file
        tenant_id: ID of the tenant
    """
    try:
        content = await file.read()
        
        # Create a BytesIO object from the content
        file_content = io.BytesIO(content)
        
        drive_loader = GoogleDriveLoader()
        result = drive_loader.save_to_google_drive(file_content, file_name, tenant_id)
        return result
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)


"""
- 문서 처리
http POST http://localhost:8005/process storage_type="drive"

- 질의
http GET http://localhost:8005/query?query="프로젝트 A의 예산이 얼마 나왔지?"
"""
