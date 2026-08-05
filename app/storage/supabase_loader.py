"""
Supabase Storage document loader and processor
"""
import os
import io
from typing import List, Optional, Dict, Any
from supabase import create_client, Client
from app.services.document_processor import get_document_processor
import asyncio

from app.core.env_loader import load_project_dotenv

load_project_dotenv(override=True)

class SupabaseStorageLoader:
    """Handles loading and processing of documents from Supabase Storage"""
    
    def __init__(self):
        """
        Initialize the Supabase Storage loader
        """
        self.supabase: Client = create_client(
            os.getenv('SUPABASE_URL'),
            os.getenv('SUPABASE_KEY')
        )
        # DocumentProcessor(청커 포함)는 실제로 파싱할 때만 필요하다. 업로드 전용 경로
        # (save-to-storage 는 upload_file_to_storage 만 씀)에서 매번 안 쓰는 processor 를 만들어
        # 청커가 이중 생성되던 것을 제거 — 파싱 시 get_document_processor() 싱글톤을 lazy 로 쓴다.
        
    async def download_and_process_file(self, file_path: str, metadata: Optional[dict] = None, tenant_id: Optional[str] = None) -> List[dict]:
        """
        Download and process a file from Supabase Storage
        
        Args:
            file_path: Path to the file in the storage
            metadata: Optional metadata to add to the documents
            
        Returns:
            List of processed documents
        """
        try:
            print(f"Processing file: {file_path}")
            
            # Get original filename from metadata
            original_filename = metadata.get('original_filename', os.path.basename(file_path)) if metadata else os.path.basename(file_path)
            
            # Download the file
            response = await asyncio.to_thread(
                self.supabase.storage.from_("files").download,
                file_path
            )
            
            # Create a BytesIO object from the response
            file_content = io.BytesIO(response)
            
            # Process the file using DocumentProcessor (공유 싱글톤 — 청커 1회 생성)
            processor = get_document_processor()
            documents = await processor.load_document(file_content, original_filename)
            if documents:
                documents = await processor.process_documents(documents, metadata or {})
            
            # Add storage metadata
            for doc in documents:
                doc.metadata.update({
                    'storage_type': 'storage',
                    'file_path': file_path,
                    'file_name': original_filename
                })
            
            return documents
                
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return []

    async def upload_image_to_storage(self, image_data: bytes, image_name: str, folder_path: str = "uploads") -> dict:
        """
        Upload an image to Supabase Storage public bucket
        
        Args:
            image_data: Image data as bytes
            image_name: Name of the image file
            folder_path: Folder path in storage (default: "uploads")
            
        Returns:
            Dictionary with upload information including public URL
        """
        try:
            # Create full path for the image
            full_path = f"{folder_path}/{image_name}"
            
            # Upload to Supabase Storage
            response = await asyncio.to_thread(
                self.supabase.storage.from_("files").upload,
                full_path,
                image_data,
                {"content-type": "image/png"}  # Adjust based on image type
            )
            
            if not response.path:
                raise Exception(f"Upload failed: {response}")
            
            # Get public URL
            public_url_response = self.supabase.storage.from_("files").get_public_url(full_path)
            public_url = public_url_response.get('publicURL', '') if isinstance(public_url_response, dict) else str(public_url_response)
            # 내부망 배포: kong:8000 등 내부 host 를 외부 접근용으로 교체 (브라우저 다운로드용).
            from app.core.config import rewrite_storage_public_host
            public_url = rewrite_storage_public_host(public_url)

            return {
                'file_id': response.path,
                'file_name': image_name,
                'public_url': public_url
            }
            
        except Exception as e:
            print(f"Error uploading image to storage: {e}")
            raise

    async def upload_file_to_storage(self, file_content: bytes, file_name: str, folder_path: str = "files", content_type: Optional[str] = None) -> dict:
        """
        Upload a file to Supabase Storage
        
        Args:
            file_content: File content as bytes
            file_name: Name of the file
            folder_path: Folder path in storage (default: "files")
            content_type: MIME type of the file (optional, will be inferred if not provided)
            
        Returns:
            Dictionary with upload information including file path and public URL
        """
        try:
            import mimetypes
            import uuid
            
            # Generate unique file name to avoid conflicts
            file_extension = os.path.splitext(file_name)[1]
            unique_file_name = f"{uuid.uuid4()}{file_extension}"
            full_path = f"{folder_path}/{unique_file_name}"
            
            # Determine content type
            if not content_type:
                content_type, _ = mimetypes.guess_type(file_name)
                if not content_type:
                    content_type = "application/octet-stream"
            
            # Upload to Supabase Storage
            response = await asyncio.to_thread(
                self.supabase.storage.from_("files").upload,
                full_path,
                file_content,
                {"content-type": content_type}
            )
            
            if not response.path:
                raise Exception(f"Upload failed: {response}")
            
            # Get public URL
            public_url_response = self.supabase.storage.from_("files").get_public_url(full_path)
            public_url = public_url_response.get('publicURL', '') if isinstance(public_url_response, dict) else str(public_url_response)
            # 내부망 배포: kong:8000 등 내부 host 를 외부 접근용으로 교체 (브라우저 다운로드용).
            from app.core.config import rewrite_storage_public_host
            public_url = rewrite_storage_public_host(public_url)

            return {
                'file_path': full_path,
                'file_name': file_name,
                'original_file_name': file_name,
                'public_url': public_url,
                'content_type': content_type
            }
            
        except Exception as e:
            print(f"Error uploading file to storage: {e}")
            raise

