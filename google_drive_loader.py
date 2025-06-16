"""
Google Drive document loader and processor
"""
import os
import io
from typing import List, Optional
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload
import asyncio

from document_loader import DocumentProcessor

class GoogleDriveLoader:
    """Handles loading and processing of documents from Google Drive"""
    
    def __init__(self, drive_service):
        """
        Initialize the Google Drive loader
        
        Args:
            drive_service: Authenticated Google Drive service instance
        """
        self.service = drive_service
        self.document_loader = DocumentProcessor()
        
    async def list_files(self, file_types: Optional[List[str]] = None, folder_id: Optional[str] = None) -> List[dict]:
        """
        List files in Google Drive, filtered by file types and folder
        
        Args:
            file_types: Optional list of file MIME types to filter by
            folder_id: Optional Google Drive folder ID
            
        Returns:
            List of file metadata dictionaries
        """
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
                response = await asyncio.to_thread(
                    self.service.files().list,
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
        
    async def download_and_process_file(self, file_id: str, file_name: str) -> List[str]:
        """
        Download a file from Google Drive and process it using DocumentLoader
        
        Args:
            file_id: Google Drive file ID
            file_name: Name of the file
            
        Returns:
            List of processed text chunks
        """
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
            chunks = await asyncio.to_thread(
                self.document_loader.load_document,
                fh,
                file_name
            )
            
            if chunks is None:
                print(f"Warning: No content extracted from {file_name}")
                return []
                
            print(f"Successfully processed {len(chunks)} chunks from {file_name}")
            return chunks
            
        except Exception as e:
            print(f"Error downloading and processing file {file_name}: {e}")
            return []
            
    async def save_to_google_drive(self, file_content: io.BytesIO, file_name: Optional[str] = None) -> dict:
        """
        Save a file to Google Drive
        
        Args:
            file_content: BytesIO object containing the file content
            file_name: file name to save
        
        Returns:
            dict: File metadata including download link
        """
        try:
            file_metadata = {
                'name': file_name if file_name else os.path.basename(file_name)
            }
            
            media = MediaIoBaseUpload(
                file_content,
                mimetype='application/octet-stream',
                resumable=True
            )
            
            file = await asyncio.to_thread(
                self.service.files().create,
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
