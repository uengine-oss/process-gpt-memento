"""API 요청/응답 Pydantic 스키마."""
from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel


class RetrieveRequest(BaseModel):
    query: str
    tenant_id: str
    proc_inst_id: Optional[str] = None
    options: Optional[dict] = None


class GoogleOAuthRequest(BaseModel):
    tenant_id: str


class GoogleTokenRequest(BaseModel):
    tenant_id: str
    access_token: str
    refresh_token: Optional[str] = None
    expires_in: Optional[int] = None
    token_type: str = "Bearer"
    scopes: Optional[list[str]] = None


class GoogleOAuthCallbackRequest(BaseModel):
    code: str
    state: str  # tenant_id
    scope: Optional[str] = None


class UploadRequest(BaseModel):
    tenant_id: str
    options: Optional[dict] = None


class RetrieveByIndicesRequest(BaseModel):
    tenant_id: str
    file_name: str
    chunk_indices: List[int]
    drive_folder_id: Optional[str] = None


class ProcessRequest(BaseModel):
    storage_type: str = "drive"
    tenant_id: str
    input_dir: Optional[str] = None
    folder_path: Optional[str] = None
    file_path: Optional[str] = None
    original_filename: Optional[str] = None
    options: Optional[dict] = None


class ProcessOutputRequest(BaseModel):
    workitem_id: str
    tenant_id: Optional[str] = None


class ProcessSessionFileRequest(BaseModel):
    """채팅 세션 첨부 파일을 Supabase storage 에서 받아 ingest.

    deepagents-lite 가 호출. fileUrl 또는 file_path 둘 중 하나 필수.
    """
    tenant_id: str
    file_url: Optional[str] = None      # Supabase public URL (https://.../storage/v1/object/public/files/{path})
    file_path: Optional[str] = None     # bucket 안 path (예: files/uuid.pdf)
    file_name: str                       # 원본 파일명
    doc_role: Optional[str] = "content"  # content / template / methodology / glossary
    # 벡터 인덱싱을 건너뛴다. 첨부를 워크스페이스의 실제 파일로 읽는 호출자(Codex)는
    # 벡터 검색을 쓰지 않으므로 임베딩이 순수 낭비이고, 임베딩 서버가 흔들리면
    # 파일 등록까지 같이 실패한다. 등록·페이지 저장은 그대로 수행한다.
    skip_vector_index: Optional[bool] = None
