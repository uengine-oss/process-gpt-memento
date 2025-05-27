"""
Google Drive document loader and processor
"""
import os
import io
from typing import List, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload

from document_loader import DocumentProcessor

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file'
]

folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

class GoogleDriveLoader:
    """Handles loading and processing of documents from Google Drive"""
    
    def __init__(self, credentials_path: str = None):
        """
        Initialize the Google Drive loader
        
        Args:
            credentials_path: Path to the service account credentials JSON file
        """
        self.credentials_path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.service = None
        self.document_loader = DocumentProcessor()
        
    def authenticate(self) -> None:
        """Authenticate with Google Drive API using service account"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=SCOPES
            )
            self.service = build('drive', 'v3', credentials=credentials)
        except Exception as e:
            print(f"Error authenticating with Google Drive: {e}")
            raise
        
    def list_files(self, file_types: Optional[List[str]] = None) -> List[dict]:
        """
        List files in Google Drive, optionally filtered by folder and file types
        
        Args:
            file_types: Optional list of file MIME types to filter by
            
        Returns:
            List of file metadata dictionaries
        """
        if not self.service:
            self.authenticate()
            
        query_parts = []
        
        if folder_id:
            query_parts.append(f"'{folder_id}' in parents")
        
        if file_types:
            mime_types = [f"mimeType='{mime_type}'" for mime_type in file_types]
            query_parts.append(f"({' or '.join(mime_types)})")
            
        query_parts.append("trashed=false")
        query = " and ".join(query_parts)
        
        results = []
        page_token = None
        
        while True:
            try:
                response = self.service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageToken=page_token
                ).execute()
                
                results.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
                    
            except Exception as e:
                print(f"Error listing files: {e}")
                break
                
        return results
        
    def download_and_process_file(self, file_id: str, file_name: str) -> List[str]:
        """
        Download a file from Google Drive and process it using DocumentLoader
        
        Args:
            file_id: Google Drive file ID
            file_name: Name of the file
            
        Returns:
            List of processed text chunks
        """
        if not self.service:
            self.authenticate()
            
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            
            while not done:
                status, done = downloader.next_chunk()
                
            fh.seek(0)
            
            # Process the file directly from memory
            print(f"Processing file: {file_name}")
            chunks = self.document_loader.load_document(fh, file_name)
            
            if chunks is None:
                print(f"Warning: No content extracted from {file_name}")
                return []
                
            print(f"Successfully processed {len(chunks)} chunks from {file_name}")
            return chunks
            
        except Exception as e:
            print(f"Error downloading and processing file {file_name}: {e}")
            return []
            
    def process_folder(self, file_types: Optional[List[str]] = None) -> List[str]:
        """
        Process all files in a Google Drive folder
        
        Args:
            file_types: Optional list of file MIME types to filter by
            
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
            
        files = self.list_files(file_types)
        all_chunks = []
        
        for file in files:
            print(f"Processing file: {file['name']} (ID: {file['id']})")
            chunks = self.download_and_process_file(file['id'], file['name'])
            if chunks is not None:
                all_chunks.extend(chunks)
            
        return all_chunks
    
    def save_to_google_drive(self, file_content: io.BytesIO, file_name: Optional[str] = None) -> dict:
        """
        Save a file to Google Drive folder
        
        Args:
            file_content: BytesIO object containing the file content
            file_name: file name to save
        
        Returns:
            dict: File metadata including download link
        """
        try:
            if not self.service:
                self.authenticate()
            
            file_metadata = {
                'name': file_name if file_name else os.path.basename(file_name),
                'parents': [folder_id]
            }
            
            media = MediaIoBaseUpload(
                file_content,
                mimetype='application/octet-stream',
                resumable=True
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink, webContentLink'
            ).execute()
                        
            return {
                'file_id': file['id'],
                'file_name': file['name'],
                'web_view_link': file['webViewLink'],
                'download_link': file['webContentLink']
            }
            
        except Exception as e:
            print(f"Error in save_to_google_drive: {e}")
            raise
