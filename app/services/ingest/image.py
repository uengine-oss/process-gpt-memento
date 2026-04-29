"""이미지 파일 → Document 변환. 필요 시 Supabase Storage 업로드."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from langchain.schema import Document

from app.storage.image_storage import get_image_storage_utils


async def process_image_file(
    file_content: bytes,
    file_name: str,
    file_id: str,
    tenant_id: str,
    proc_inst_id: Optional[str] = None,
    storage_type: str = "storage",
    storage_file_path: Optional[str] = None,
    public_url: Optional[str] = None,
) -> Optional[List[Document]]:
    try:
        file_extension = Path(file_name).suffix.lower()
        image_extensions = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]

        if file_extension not in image_extensions:
            print(f"Unsupported image file type: {file_extension}")
            return None

        if storage_file_path and public_url:
            image_url = public_url
        else:
            storage_utils = get_image_storage_utils()
            upload_result = await storage_utils.upload_image_to_storage(
                file_content,
                file_name,
            )
            if not upload_result:
                print(f"Failed to upload image {file_name}")
                return None
            image_url = upload_result.get("public_url")

        metadata = {
            "file_id": file_id,
            "file_name": file_name,
            "tenant_id": tenant_id,
            "storage_type": storage_type,
            "image_count": 1,
            "source": file_name,
            "file_type": file_extension[1:],
            "image_url": image_url,
        }
        if proc_inst_id:
            metadata["proc_inst_id"] = proc_inst_id

        return [Document(page_content="", metadata=metadata)]

    except Exception as e:
        print(f"Error processing image file {file_name}: {e}")
        return None
