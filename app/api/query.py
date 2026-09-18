"""Drive 저장 라우터: /save-to-drive."""
from __future__ import annotations

import io
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from app.core.auth import create_auth_error_response
from app.core.supabase_client import supabase
from app.services.google_drive_loader import GoogleDriveLoader

router = APIRouter()


@router.post("/save-to-drive")
async def save_to_drive(
    file: UploadFile = File(...),
    file_name: str = Form(...),
    tenant_id: str = Form(...),
    folder_path: Optional[str] = Form(None),
):
    """Google Drive 업로드 (인제스트 없음)."""
    try:
        content = await file.read()
        file_content = io.BytesIO(content)
        drive_loader = GoogleDriveLoader(tenant_id=tenant_id)

        try:
            return await drive_loader.save_to_google_drive(file_content, file_name, folder_path=folder_path)
        except ValueError as e:
            if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                auth_response = create_auth_error_response(
                    supabase, tenant_id, "Google Drive authentication required to upload files"
                )
                return JSONResponse(status_code=401, content=auth_response)
            raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
