"""
Image storage utilities for Supabase Storage
"""
import os
import asyncio
from typing import Dict, Any
from supabase import create_client, Client

class ImageStorageUtils:
    """Handles image upload to Supabase Storage"""
    
    def __init__(self):
        """Initialize the image storage utility"""
        self.supabase: Client = create_client(
            os.getenv('SUPABASE_URL'),
            os.getenv('SUPABASE_KEY')
        )
        
    async def upload_image_to_storage(self, image_data: bytes, image_name: str, folder_path: str = "extracted_images") -> Dict[str, Any]:
        """
        Upload an image to Supabase Storage public bucket
        
        Args:
            image_data: Image data as bytes
            image_name: Name of the image file
            folder_path: Folder path in storage (default: "extracted_images")
            
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
            
            return {
                'file_id': response.path,
                'file_name': image_name,
                'public_url': public_url
            }
            
        except Exception as e:
            print(f"Error uploading image to storage: {e}")
            raise

    async def upload_images_batch(self, images: list, tenant_id: str, file_id: str) -> list:
        """
        Upload multiple images to Supabase Storage in batch
        
        Args:
            images: List of image dictionaries with image_data, image_name, etc.
            tenant_id: Tenant ID for folder organization
            file_id: File ID for folder organization
            
        Returns:
            List of uploaded image information
        """
        uploaded_images = []
        
        for image in images:
            try:
                # Upload to storage
                upload_result = await self.upload_image_to_storage(
                    image['image_data'],
                    image['image_name'],
                    f"extracted_images/{tenant_id}/{file_id}"
                )
                
                # Add original image metadata
                uploaded_images.append({
                    'image_id': image['image_id'],
                    'image_name': image['image_name'],
                    'image_url': upload_result.get('public_url'),
                    'metadata': image['metadata']
                })
                
            except Exception as e:
                print(f"Error uploading image {image.get('image_name', 'unknown')}: {e}")
                continue
                
        return uploaded_images
