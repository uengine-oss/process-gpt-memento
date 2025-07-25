"""
Supabase Storage document loader and processor
"""
import os
import io
from typing import List, Optional, Dict, Any
from supabase import create_client, Client
from document_loader import DocumentProcessor
import asyncio

from dotenv import load_dotenv

load_dotenv(override=True)

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
        self.document_processor = DocumentProcessor()
        
    async def download_and_process_file(self, file_path: str, metadata: Optional[dict] = None) -> List[dict]:
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
            
            # Process the file using DocumentProcessor
            documents = await self.document_processor.load_document(file_content, original_filename)
            if documents:
                documents = await self.document_processor.process_documents(documents, metadata or {})
            
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

