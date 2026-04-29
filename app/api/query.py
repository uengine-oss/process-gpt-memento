"""질의/저장 라우터: /query, /save-to-drive."""
from __future__ import annotations

import io
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from app.core.auth import create_auth_error_response
from app.core.supabase_client import supabase
from app.services.google_drive_loader import GoogleDriveLoader
from app.services.rag_chain import get_rag_chain

router = APIRouter()


@router.get("/query")
async def answer_query(query: str, tenant_id: str):
    """RAG 체인으로 질의 응답."""
    try:
        rag = get_rag_chain()
        metadata_filter = {"tenant_id": tenant_id}
        result = await rag.answer(query, metadata_filter)
        return {
            "response": result["answer"],
            "metadata": {
                f"{doc.metadata.get('file_name', 'unknown')}#{doc.metadata.get('chunk_index', i)}": doc.metadata
                for i, doc in enumerate(result["source_documents"])
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
