from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2AuthorizationCodeBearer
from pydantic import BaseModel
from typing import Optional, Dict, Any
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
from auth import get_current_user

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
    input_dir: Optional[str] = None
    folder_path: Optional[str] = None
    file_path: Optional[str] = None
    original_filename: Optional[str] = None
    options: Optional[dict] = None

class RetrieveRequest(BaseModel):
    query: str
    options: Optional[dict] = None

class GoogleOAuthRequest(BaseModel):
    tenant_id: str

class GoogleTokenRequest(BaseModel):
    tenant_id: str
    user_email: str
    access_token: str
    refresh_token: Optional[str] = None
    expires_in: Optional[int] = None
    token_type: str = "Bearer"
    scopes: Optional[list[str]] = None

class GoogleOAuthCallbackRequest(BaseModel):
    code: str
    state: str  # This should be the tenant_id
    scope: Optional[str] = None

@app.get("/auth/google/url")
async def get_google_auth_url(tenant_id: str):
    """Get Google OAuth authorization URL for the tenant"""
    try:
        # Get OAuth settings from database
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .eq("provider", "google") \
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
            'state': tenant_id  # Use tenant_id as state
        }
        
        auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
        
        return {
            "auth_url": auth_url,
            "state": tenant_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/auth/google/status")
async def get_google_auth_status(
    tenant_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Check if user has valid Google OAuth tokens"""
    try:
        response = supabase.table("users") \
            .select("google_credentials, google_credentials_updated_at") \
            .eq("email", current_user.email) \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()
        
        if not response.data or not response.data.get('google_credentials'):
            return {"authenticated": False, "message": "No Google credentials found"}
        
        # Parse token data
        token_data = json.loads(response.data['google_credentials'])
        
        # Check if token is expired
        if token_data.get('expiry'):
            expiry = datetime.fromisoformat(token_data['expiry'])
            if datetime.utcnow() > expiry:
                return {"authenticated": False, "message": "Token expired"}
        
        return {
            "authenticated": True,
            "user_email": current_user.email,
            "expires_at": token_data.get('expiry'),
            "updated_at": response.data.get('google_credentials_updated_at')
        }
        
    except Exception as e:
        return {"authenticated": False, "message": str(e)}

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
        # Use user-specific authentication
        drive_loader = GoogleDriveLoader(
            tenant_id=current_user.app_metadata['tenant_id'],
            user_email=current_user.email
        )
        
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

        try:
            # Get list of files from Google Drive
            files = await drive_loader.list_files(supported_mime_types)
        except ValueError as e:
            # Authentication error - return login URL
            if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                auth_response = create_auth_error_response(
                    current_user.app_metadata['tenant_id'],
                    current_user.email,
                    "Google Drive authentication required to process documents"
                )
                return JSONResponse(
                    status_code=401,
                    content=auth_response
                )
            else:
                raise e
        
        if not files:
            return {"message": f"No documents found in Google Drive folder for user {current_user.email}"}
            
        rag = RAGChain()
        
        # Get list of already processed files for this user
        processed_files = await rag.get_processed_files(current_user.app_metadata['tenant_id'])
        
        # Filter out already processed files
        new_files = [f for f in files if f['id'] not in processed_files]
        
        if not new_files:
            return {"message": f"No new documents to process for user {current_user.email}"}
            
        # Process only new files
        all_documents = []
        for file in new_files:
            try:
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
            except Exception as e:
                # Skip files that can't be processed
                continue
        
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
            return {"message": f"No new content to process for user {current_user.email}"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_supabase_storage(request: ProcessRequest, current_user: Dict[str, Any]):
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
        
        # Use user-specific authentication
        drive_loader = GoogleDriveLoader(
            tenant_id=current_user.app_metadata['tenant_id'],
            user_email=current_user.email
        )
        
        try:
            result = await drive_loader.save_to_google_drive(file_content, file_name)
            return result
        except ValueError as e:
            # Authentication error - return login URL
            if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                auth_response = create_auth_error_response(
                    current_user.app_metadata['tenant_id'],
                    current_user.email,
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
    """Save Google OAuth token to user's google_credentials column"""
    try:
        # Check if user exists first
        user_check = supabase.table("users").select("id, email").eq("email", request.user_email).eq("tenant_id", request.tenant_id).single().execute()
        
        if not user_check.data:
            raise HTTPException(status_code=404, detail=f"User {request.user_email} not found in tenant {request.tenant_id}")
        
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
        
        # Update user's google_credentials
        response = supabase.table("users").upsert({
            "google_credentials": json.dumps(token_data),
            "google_credentials_updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("email", request.user_email).eq("tenant_id", request.tenant_id).execute()
        
        if not response.data:
            raise HTTPException(status_code=500, detail="Failed to update user credentials")

        return {
            "message": "Google token saved successfully",
            "user_email": request.user_email,
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
            .eq("provider", "google") \
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
        
        # Get user info from Google to get email
        user_info_url = "https://www.googleapis.com/oauth2/v2/userinfo"
        headers = {'Authorization': f"Bearer {token_info['access_token']}"}
        
        async with httpx.AsyncClient() as client:
            user_response = await client.get(user_info_url, headers=headers)

            if user_response.status_code != 200:
                # Try alternative approach - use ID token if available
                if 'id_token' in token_info:
                    try:
                        import jwt
                        # Decode ID token (without verification for now)
                        id_token_data = jwt.decode(token_info['id_token'], options={"verify_signature": False})
                        user_info = {
                            'email': id_token_data.get('email'),
                            'name': id_token_data.get('name'),
                            'picture': id_token_data.get('picture')
                        }
                    except Exception as jwt_error:
                        raise HTTPException(
                            status_code=400, 
                            detail=f"Failed to get user info: {user_response.text}"
                        )
                else:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Failed to get user info: {user_response.text}"
                    )
            else:
                user_info = user_response.json()
        
        if not user_info.get('email'):
            raise HTTPException(
                status_code=400,
                detail="User email not found in user info"
            )
        
        # Save token using existing endpoint
        token_request = GoogleTokenRequest(
            tenant_id=tenant_id,
            user_email=user_info['email'],
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
            "user_email": user_info['email'],
            "tenant_id": tenant_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def create_auth_error_response(tenant_id: str, user_email: str, error_message: str = "Authentication required"):
    """Create a standardized auth error response with login URL"""
    try:
        # Get OAuth settings to create login URL
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .eq("provider", "google") \
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
                "tenant_id": tenant_id,
                "user_email": user_email
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
http GET http://localhost:8005/query?query="프로젝트 A의 예산이 얼마 나왔지?"
"""
