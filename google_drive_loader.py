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

import fitz  # PyMuPDF for PDF image extraction
from PIL import Image
import io
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile'
]

class GoogleDriveLoader:
    """Handles loading and processing of documents from Google Drive"""
    
    def __init__(self, drive_service=None, token_path: str = 'token.json', tenant_id: Optional[str] = None):
        """
        Initialize the Google Drive loader
        
        Args:
            drive_service: Optional authenticated Google Drive service instance
            token_path: Path to save/load the token.json file (used when drive_service is None)
            tenant_id: Tenant ID to get OAuth settings from database
        """
        self.token_path = token_path
        self.credentials = None
        self.service = drive_service
        self.document_loader = DocumentProcessor()
        self.tenant_id = tenant_id
        
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
            response = self.supabase.table("tenant_oauth") \
                .select("*") \
                .eq("tenant_id", self.tenant_id) \
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
                .single() \
                .execute()
            
            if response.data:
                return response.data.get("drive_folder_id")
            return None
        except Exception as e:
            print(f"Error fetching folder ID: {e}")
            return None
        
    async def authenticate(self) -> None:
        """Authenticate with Google Drive API using tenant-level tokens"""
        if self.service:
            return  # Already authenticated
            
        if not self.tenant_id:
            raise ValueError("tenant_id is required for database-based authentication")
            
        try:
            if self.supabase:
                try:
                    # Get tenant's Google credentials from tenant_oauth table
                    response = self.supabase.table("tenant_oauth") \
                        .select("google_credentials, client_id, client_secret") \
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
                                    client_id = response.data.get('client_id')
                                    client_secret = response.data.get('client_secret')
                                    
                                    if client_id and client_secret:
                                        creds = Credentials(
                                            token=token_data['access_token'],
                                            refresh_token=token_data['refresh_token'],
                                            token_uri="https://oauth2.googleapis.com/token",
                                            client_id=client_id,
                                            client_secret=client_secret,
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
                                            
                                            self.supabase.table("tenant_oauth") \
                                                .update({
                                                    "google_credentials": json.dumps(updated_token_data),
                                                    "google_credentials_updated_at": datetime.now(timezone.utc).isoformat()
                                                }) \
                                                .eq("tenant_id", self.tenant_id) \
                                                .execute()
                                        
                                        self.credentials = creds
                                        self.service = build('drive', 'v3', credentials=self.credentials)
                                        print(f"Successfully authenticated using tenant token for {self.tenant_id}")
                                        return
                                    else:
                                        print(f"Client settings not found for tenant {self.tenant_id}")
                                else:
                                    print(f"Token expired and no refresh token available for tenant {self.tenant_id}")
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
                                print(f"Successfully authenticated using tenant token for {self.tenant_id}")
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
                            print(f"Successfully authenticated using tenant token for {self.tenant_id}")
                            return
                                
                except Exception as e:
                    print(f"Tenant token authentication failed: {e}")
            
            # Fallback: raise error if no tenant token available
            raise ValueError(f"No valid Google credentials found for tenant {self.tenant_id}. Please authenticate with Google first.")
            
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
                
                # Execute the request in a separate thread with timeout
                response = await asyncio.wait_for(
                    asyncio.to_thread(request.execute),
                    timeout=30.0  # 30초 타임아웃 설정
                )
                
                results.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
                    
            except asyncio.TimeoutError:
                print("Request timeout - retrying...")
                continue
            except Exception as e:
                print(f"Error listing files: {e}")
                if "Unable to find the server" in str(e):
                    print("Network connection issue detected. Please check your internet connection.")
                break
                
        return results
        
    async def download_and_process_file(self, file_id: str, file_name: str, tenant_id: Optional[str] = None) -> Optional[List[Document]]:
        """파일을 다운로드하고 처리하며 이미지도 추출 - 개선된 버전"""
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            
            while not done:
                status, done = await asyncio.to_thread(downloader.next_chunk)
                
            fh.seek(0)
            file_content = fh.read()
            
            # 이미지 추출 (document_loader 사용)
            from document_loader import DocumentProcessor
            from image_storage_utils import ImageStorageUtils
            
            doc_processor = DocumentProcessor()
            extracted_images = await doc_processor.extract_images_from_document(
                file_content, 
                file_name, 
                file_id
            )
            
            # Supabase Storage에 이미지 업로드
            if extracted_images:
                storage_utils = ImageStorageUtils()
                uploaded_images = await storage_utils.upload_images_batch(
                    extracted_images, 
                    tenant_id, 
                    file_id
                )
                extracted_images = uploaded_images
            
            # 문서 처리
            fh.seek(0)
            documents = await self.document_loader.load_document(fh, file_name)
            
            if documents:
                # 이미지 정보를 메타데이터에 추가 (원본 데이터 포함)
                for doc in documents:
                    doc.metadata.update({
                        'extracted_images': extracted_images,
                        'image_count': len(extracted_images),
                        'file_id': file_id,
                        'file_name': file_name,
                        'tenant_id': tenant_id,
                        'storage_type': 'drive',
                        # 원본 이미지 데이터가 포함된 extracted_images 사용
                    })
                
                # 청크로 분할
                chunks = await self.document_loader.process_documents(documents, {
                    'file_id': file_id,
                    'storage_type': 'drive'
                })
                
                return chunks
            
            return None
            
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")
            return None
            
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
            chunks = await self.download_and_process_file(file['id'], file['name'], self.tenant_id)
            if chunks is not None:
                all_chunks.extend(chunks)
            
        return all_chunks
            
    async def save_to_google_drive(self, file_content: io.BytesIO, file_name: Optional[str] = None, folder_id: Optional[str] = None, folder_path: Optional[str] = None) -> dict:
        """
        Save a file to Google Drive
        
        Args:
            file_content: BytesIO object containing the file content
            file_name: file name to save
            folder_id: Optional Google Drive folder ID to save to (if not provided, uses tenant's default folder)
            folder_path: Optional folder path like "프로세스 정의/년/월/일/인스턴스/source/" (if provided, creates folders as needed)
        
        Returns:
            dict: File metadata including download link
        """
        try:
            if not self.service:
                await self.authenticate()
            
            # If folder_path is provided, get or create the folder path
            if folder_path:
                folder_id = await self.get_or_create_folder_path(folder_path)
            # If folder_id is not provided and no folder_path, try to get it from database
            elif not folder_id:
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
                'download_url': file['webContentLink']
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

    async def create_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """
        Create a folder in Google Drive
        
        Args:
            folder_name: Name of the folder to create
            parent_folder_id: Optional parent folder ID (if not provided, uses tenant's default folder)
            
        Returns:
            str: ID of the created folder
        """
        try:
            if not self.service:
                await self.authenticate()
            
            # If parent_folder_id is not provided, try to get it from database
            if not parent_folder_id:
                parent_folder_id = await self.get_tenant_folder_id()
            
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            # Create the request first
            request = self.service.files().create(
                body=folder_metadata,
                fields='id, name'
            )
            
            # Execute the request in a separate thread
            folder = await asyncio.to_thread(request.execute)
            
            return folder['id']
            
        except Exception as e:
            print(f"Error creating folder {folder_name}: {e}")
            raise
    
    async def get_or_create_folder_path(self, folder_path: str) -> str:
        """
        Get or create a folder path in Google Drive
        
        Args:
            folder_path: Path like "프로세스 정의/년/월/일/인스턴스/source/"
            
        Returns:
            str: ID of the final folder in the path
        """
        try:
            if not self.service:
                await self.authenticate()
            
            # Get the root folder ID from tenant settings
            root_folder_id = await self.get_tenant_folder_id()
            if not root_folder_id:
                raise ValueError("No root folder ID found for tenant")
            
            # Split the path and remove empty strings
            path_parts = [part.strip() for part in folder_path.split('/') if part.strip()]
            
            current_folder_id = root_folder_id
            
            # Navigate through the path, creating folders as needed
            for folder_name in path_parts:
                # Check if folder exists in current parent
                existing_folder_id = await self.find_folder_in_parent(folder_name, current_folder_id)
                
                if existing_folder_id:
                    # Folder exists, use it
                    current_folder_id = existing_folder_id
                else:
                    # Folder doesn't exist, create it
                    current_folder_id = await self.create_folder(folder_name, current_folder_id)
            
            return current_folder_id
            
        except Exception as e:
            print(f"Error getting or creating folder path {folder_path}: {e}")
            raise
    
    async def find_folder_in_parent(self, folder_name: str, parent_folder_id: str) -> Optional[str]:
        """
        Find a folder with the given name in a parent folder
        
        Args:
            folder_name: Name of the folder to find
            parent_folder_id: ID of the parent folder
            
        Returns:
            Optional[str]: ID of the found folder, or None if not found
        """
        try:
            if not self.service:
                await self.authenticate()
            
            # Search for folders with the given name in the parent folder
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                return files[0]['id']  # Return the first matching folder
            
            return None
            
        except Exception as e:
            print(f"Error finding folder {folder_name} in parent {parent_folder_id}: {e}")
            return None
        



    async def save_image_to_drive(self, image_io: io.BytesIO, image_name: str, folder_path: str) -> dict:
        """추출된 이미지를 Google Drive에 저장"""
        try:
            if not self.service:
                await self.authenticate()
            
            # 이미지용 폴더 생성
            folder_id = await self.get_or_create_folder_path(folder_path)
            
            # 이미지 메타데이터 설정
            file_metadata = {
                'name': image_name,
                'parents': [folder_id] if folder_id else None
            }
            
            media = MediaIoBaseUpload(
                image_io,
                mimetype='image/png',  # 또는 적절한 MIME 타입
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
                'download_url': file['webContentLink']
            }
            
        except Exception as e:
            print(f"Error saving image to drive: {e}")
            raise

    async def download_file_content(self, file_id: str) -> bytes:
        """파일의 원본 내용을 바이트로 다운로드"""
        try:
            if not self.service:
                await self.authenticate()
            
            request = self.service.files().get_media(fileId=file_id)
            file_content = await asyncio.to_thread(request.execute)
            
            return file_content
            
        except Exception as e:
            print(f"Error downloading file content: {e}")
            raise

    async def get_file_info(self, file_id: str) -> Optional[dict]:
        """Get file information from Google Drive"""
        if not self.service:
            await self.authenticate()
            
        try:
            # Execute the request in a separate thread
            file_info = await asyncio.to_thread(
                self.service.files().get(
                    fileId=file_id,
                    fields='id,name,mimeType,size,modifiedTime'
                ).execute
            )
            return file_info
        except Exception as e:
            print(f"Error getting file info for {file_id}: {e}")
            return None
