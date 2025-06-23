"""
Google Drive document loader and processor
"""
import os
import io
from typing import List, Optional, Dict, Any
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload
import asyncio
from supabase import create_client, Client
from dotenv import load_dotenv
import json
from langchain.schema import Document
from datetime import datetime

from document_loader import DocumentProcessor

load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file'  # For file upload
]

class GoogleDriveLoader:
    """Handles loading and processing of documents from Google Drive"""
    
    def __init__(self, drive_service=None, token_path: str = 'token.json', tenant_id: Optional[str] = None, user_email: Optional[str] = None):
        """
        Initialize the Google Drive loader
        
        Args:
            drive_service: Optional authenticated Google Drive service instance
            token_path: Path to save/load the token.json file (used when drive_service is None)
            tenant_id: Tenant ID to get OAuth settings from database
            user_email: User email to get user-specific tokens from database
        """
        self.token_path = token_path
        self.credentials = None
        self.service = drive_service
        self.document_loader = DocumentProcessor()
        self.tenant_id = tenant_id
        self.user_email = user_email
        
        # Initialize Supabase client if tenant_id is provided
        if tenant_id:
            self.supabase: Client = create_client(
                os.getenv("SUPABASE_URL"),
                os.getenv("SUPABASE_KEY")
            )
        else:
            self.supabase = None
        
    async def get_tenant_oauth_settings(self) -> Dict[str, Any]:
        """Get OAuth settings for the current tenant from database"""
        if not self.tenant_id or not self.supabase:
            raise ValueError("tenant_id is required to get OAuth settings from database")
            
        try:
            # First try to get user-specific tokens if user_email is provided
            if self.user_email:
                response = self.supabase.table("tenant_oauth") \
                    .select("*") \
                    .eq("tenant_id", self.tenant_id) \
                    .eq("provider", "google") \
                    .eq("user_email", self.user_email) \
                    .single() \
                    .execute()
                
                if response.data:
                    return response.data
            
            # Fallback to tenant-level settings (no user_email)
            response = self.supabase.table("tenant_oauth") \
                .select("*") \
                .eq("tenant_id", self.tenant_id) \
                .eq("provider", "google") \
                .is_("user_email", "null") \
                .single() \
                .execute()
            
            if not response.data:
                raise ValueError(f"OAuth settings not found for tenant: {self.tenant_id}")
                
            return response.data
        except Exception as e:
            raise ValueError(f"Error fetching OAuth settings: {str(e)}")
    
    async def get_tenant_folder_id(self) -> Optional[str]:
        """Get Google Drive folder ID for the current tenant from database"""
        if not self.tenant_id or not self.supabase:
            return None
            
        try:
            response = self.supabase.table("tenant_oauth") \
                .select("drive_folder_id") \
                .eq("tenant_id", self.tenant_id) \
                .eq("provider", "google") \
                .single() \
                .execute()
            
            if response.data:
                return response.data.get("drive_folder_id")
            return None
        except Exception as e:
            print(f"Error fetching folder ID: {e}")
            return None
        
    async def authenticate(self) -> None:
        """Authenticate with Google Drive API using user-specific tokens"""
        if self.service:
            return  # Already authenticated
            
        if not self.tenant_id:
            raise ValueError("tenant_id is required for database-based authentication")
            
        try:
            # First, try to use user-specific tokens if user_email is provided
            if self.user_email and self.supabase:
                try:
                    # Get user's Google credentials from users table
                    response = self.supabase.table("users") \
                        .select("google_credentials") \
                        .eq("email", self.user_email) \
                        .eq("tenant_id", self.tenant_id) \
                        .single() \
                        .execute()
                    
                    if response.data and response.data.get('google_credentials'):
                        token_data = json.loads(response.data['google_credentials'])
                        
                        # Check if token is expired
                        from datetime import datetime, timezone
                        if token_data.get('expiry'):
                            expiry = datetime.fromisoformat(token_data['expiry'])
                            if datetime.now(timezone.utc) > expiry:
                                # Token expired, try to refresh
                                if token_data.get('refresh_token'):
                                    from google.oauth2.credentials import Credentials
                                    from google.auth.transport.requests import Request
                                    
                                    # Get client settings for refresh
                                    client_response = self.supabase.table("tenant_oauth") \
                                        .select("client_id, client_secret") \
                                        .eq("tenant_id", self.tenant_id) \
                                        .eq("provider", "google") \
                                        .single() \
                                        .execute()
                                    
                                    if client_response.data:
                                        creds = Credentials(
                                            token=token_data['access_token'],
                                            refresh_token=token_data['refresh_token'],
                                            token_uri="https://oauth2.googleapis.com/token",
                                            client_id=client_response.data['client_id'],
                                            client_secret=client_response.data['client_secret'],
                                            scopes=token_data.get('scopes', SCOPES)
                                        )
                                        
                                        if creds.expired and creds.refresh_token:
                                            creds.refresh(Request())
                                            
                                            # Update token in database
                                            updated_token_data = {
                                                "access_token": creds.token,
                                                "refresh_token": token_data.get('refresh_token'),
                                                "token_type": token_data.get('token_type', 'Bearer'),
                                                "expires_in": token_data.get('expires_in'),
                                                "scopes": token_data.get('scopes', SCOPES)
                                            }
                                            
                                            if creds.expiry:
                                                updated_token_data["expiry"] = creds.expiry.isoformat()
                                            
                                            self.supabase.table("users") \
                                                .update({
                                                    "google_credentials": json.dumps(updated_token_data),
                                                    "google_credentials_updated_at": datetime.now(timezone.utc).isoformat()
                                                }) \
                                                .eq("email", self.user_email) \
                                                .eq("tenant_id", self.tenant_id) \
                                                .execute()
                                        
                                        self.credentials = creds
                                        self.service = build('drive', 'v3', credentials=self.credentials)
                                        print(f"Successfully authenticated using user token for {self.user_email}")
                                        return
                                    else:
                                        print(f"Client settings not found for tenant {self.tenant_id}")
                                else:
                                    print(f"Token expired and no refresh token available for {self.user_email}")
                            else:
                                # Token is still valid
                                from google.oauth2.credentials import Credentials
                                
                                self.credentials = Credentials(
                                    token=token_data['access_token'],
                                    refresh_token=token_data.get('refresh_token'),
                                    token_uri="https://oauth2.googleapis.com/token",
                                    scopes=token_data.get('scopes', SCOPES)
                                )
                                
                                self.service = build('drive', 'v3', credentials=self.credentials)
                                print(f"Successfully authenticated using user token for {self.user_email}")
                                return
                        else:
                            # No expiry info, assume token is valid
                            from google.oauth2.credentials import Credentials
                            
                            self.credentials = Credentials(
                                token=token_data['access_token'],
                                refresh_token=token_data.get('refresh_token'),
                                token_uri="https://oauth2.googleapis.com/token",
                                scopes=token_data.get('scopes', SCOPES)
                            )
                            
                            self.service = build('drive', 'v3', credentials=self.credentials)
                            print(f"Successfully authenticated using user token for {self.user_email}")
                            return
                                
                except Exception as e:
                    print(f"User token authentication failed: {e}, falling back to other methods")
            
            # Fallback: raise error if no user token available
            raise ValueError(f"No valid Google credentials found for user {self.user_email}. Please authenticate with Google first.")
            
        except Exception as e:
            raise ValueError(f"Authentication failed: {str(e)}")
        
    async def list_files(self, file_types: Optional[List[str]] = None, folder_id: Optional[str] = None) -> List[dict]:
        """
        List files in Google Drive, filtered by file types and folder
        
        Args:
            file_types: Optional list of file MIME types to filter by
            folder_id: Optional Google Drive folder ID (if not provided, uses tenant's default folder)
            
        Returns:
            List of file metadata dictionaries
        """
        if not self.service:
            await self.authenticate()
            
        # If folder_id is not provided, try to get it from database
        if not folder_id:
            folder_id = await self.get_tenant_folder_id()
            
        query_parts = []
        
        if file_types:
            mime_types = [f"mimeType='{mime_type}'" for mime_type in file_types]
            query_parts.append(f"({' or '.join(mime_types)})")
            
        if folder_id:
            query_parts.append(f"trashed=false and '{folder_id}' in parents")
        else:
            query_parts.append("trashed=false")
        
        query = " and ".join(query_parts)
        
        results = []
        page_token = None
        
        while True:
            try:
                # Create the request first
                request = self.service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageToken=page_token
                )
                
                # Execute the request in a separate thread
                response = await asyncio.to_thread(request.execute)
                
                results.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
                    
            except Exception as e:
                print(f"Error listing files: {e}")
                break
                
        return results
        
    async def download_and_process_file(self, file_id: str, file_name: str) -> List[Document]:
        """
        Download a file from Google Drive and process it using DocumentLoader
        
        Args:
            file_id: Google Drive file ID
            file_name: Name of the file
            
        Returns:
            List of processed Document objects
        """
        if not self.service:
            await self.authenticate()
            
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            
            while not done:
                status, done = await asyncio.to_thread(downloader.next_chunk)
                
            fh.seek(0)
            
            # Process the file directly from memory
            print(f"Processing file: {file_name}")
            documents = await self.document_loader.load_document(fh, file_name)
            
            if documents is None:
                print(f"Warning: No content extracted from {file_name}")
                return []
                
            print(f"Successfully processed {len(documents)} documents from {file_name}")
            return documents
            
        except Exception as e:
            print(f"Error downloading and processing file {file_name}: {e}")
            return []
            
    async def process_folder(self, file_types: Optional[List[str]] = None, folder_id: Optional[str] = None) -> List[str]:
        """
        Process all files in a Google Drive folder
        
        Args:
            file_types: Optional list of file MIME types to filter by
            folder_id: Optional Google Drive folder ID (if not provided, uses tenant's default folder)
            
        Returns:
            List of all processed text chunks from all files
        """
        if file_types is None:
            file_types = [
                'application/vnd.google-apps.document',  # Google Docs
                'application/vnd.google-apps.spreadsheet',  # Google Sheets
                'application/vnd.google-apps.presentation',  # Google Slides
                'application/pdf',  # PDF
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # DOCX
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # XLSX
                'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # PPTX
                'text/plain',  # TXT
                'application/vnd.google-apps.folder'  # Google Drive 폴더
            ]
            
        files = await self.list_files(file_types, folder_id)
        all_chunks = []
        
        for file in files:
            print(f"Processing file: {file['name']} (ID: {file['id']})")
            chunks = await self.download_and_process_file(file['id'], file['name'])
            if chunks is not None:
                all_chunks.extend(chunks)
            
        return all_chunks
            
    async def save_to_google_drive(self, file_content: io.BytesIO, file_name: Optional[str] = None, folder_id: Optional[str] = None) -> dict:
        """
        Save a file to Google Drive
        
        Args:
            file_content: BytesIO object containing the file content
            file_name: file name to save
            folder_id: Optional Google Drive folder ID to save to (if not provided, uses tenant's default folder)
        
        Returns:
            dict: File metadata including download link
        """
        try:
            if not self.service:
                await self.authenticate()
            
            # If folder_id is not provided, try to get it from database
            if not folder_id:
                folder_id = await self.get_tenant_folder_id()
            
            file_metadata = {
                'name': file_name if file_name else os.path.basename(file_name)
            }
            
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            media = MediaIoBaseUpload(
                file_content,
                mimetype='application/octet-stream',
                resumable=True
            )
            
            # Create the request first
            request = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink, webContentLink'
            )
            
            # Execute the request in a separate thread
            file = await asyncio.to_thread(request.execute)
                        
            return {
                'file_id': file['id'],
                'file_name': file['name'],
                'web_view_link': file['webViewLink'],
                'download_link': file['webContentLink']
            }
            
        except ValueError as e:
            # Re-raise authentication errors
            if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                raise e
            else:
                print(f"Error in save_to_google_drive: {e}")
                raise
        except Exception as e:
            print(f"Error in save_to_google_drive: {e}")
            raise
