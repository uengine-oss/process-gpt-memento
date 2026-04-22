from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2AuthorizationCodeBearer
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os
import io
import gc
import json
import httpx
import tracemalloc
from datetime import datetime, timedelta
from urllib.parse import urlencode

from supabase import create_client, Client

from google_drive_loader import GoogleDriveLoader
from rag_chain import RAGChain, get_rag_chain
from llm import log_provider_config
from chunkers import log_active_strategy as log_chunker_strategy
from retrievers import log_active_strategy as log_retriever_strategy

from auth_utils import create_auth_error_response
from ingest_router import router as ingest_router


app = FastAPI(title="Memento Service API", description="API for document processing and querying")

@app.on_event("startup")
async def _log_startup_config():
    log_provider_config()
    log_chunker_strategy()
    log_retriever_strategy()
    if os.getenv("MEMENTO_TRACEMALLOC", "0").strip().lower() in {"1", "true", "yes", "on"}:
        if not tracemalloc.is_tracing():
            tracemalloc.start(25)
            print("tracemalloc started (depth=25)")


def _read_rss_mb() -> Optional[float]:
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except Exception:
        return None
    return None


@app.get("/debug/memory")
async def debug_memory(top: int = 15, snapshot: bool = False):
    gc.collect()

    drive_jobs_info: Dict[str, Any]
    try:
        from ingest_router import drive_jobs, tenant_active_job
        running = sum(1 for j in drive_jobs.values() if j.get("status") == "running")
        drive_jobs_info = {
            "total": len(drive_jobs),
            "running": running,
            "completed_or_failed": len(drive_jobs) - running,
            "active_tenants": len(tenant_active_job),
        }
    except Exception as e:
        drive_jobs_info = {"error": str(e)}

    singletons: Dict[str, Any]
    try:
        import rag_chain as _rag_mod
        import vector_store as _vs_mod
        import document_loader as _dl_mod
        import image_storage_utils as _is_mod
        singletons = {
            "rag_chain": _rag_mod._rag_chain_instance is not None,
            "vector_store": _vs_mod._vector_store_instance is not None,
            "document_processor": _dl_mod._document_processor_instance is not None,
            "image_storage": _is_mod._image_storage_instance is not None,
        }
    except Exception as e:
        singletons = {"error": str(e)}

    result: Dict[str, Any] = {
        "rss_mb": _read_rss_mb(),
        "gc_objects": len(gc.get_objects()),
        "gc_stats": gc.get_stats(),
        "drive_jobs": drive_jobs_info,
        "singletons": singletons,
        "tracemalloc_enabled": tracemalloc.is_tracing(),
    }

    if snapshot and tracemalloc.is_tracing():
        stats = tracemalloc.take_snapshot().statistics("lineno")[:top]
        result["tracemalloc_top"] = [
            {
                "file": str(s.traceback[0].filename),
                "line": s.traceback[0].lineno,
                "size_kb": round(s.size / 1024, 1),
                "count": s.count,
            }
            for s in stats
        ]

    return result

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl="https://accounts.google.com/o/oauth2/auth",
    tokenUrl="https://oauth2.googleapis.com/token"
)

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# Ingest (parse/chunk/embed) endpoints live in ingest_router.py
app.include_router(ingest_router)


# ---------------------------------------------------------------------------
# Pydantic models (non-ingest)
# ---------------------------------------------------------------------------

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
    state: str  # This should be the tenant_id
    scope: Optional[str] = None


class UploadRequest(BaseModel):
    tenant_id: str
    options: Optional[dict] = None


class RetrieveByIndicesRequest(BaseModel):
    tenant_id: str
    file_name: str
    chunk_indices: List[int]
    drive_folder_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Google OAuth
# ---------------------------------------------------------------------------

@app.get("/auth/google/url")
async def get_google_auth_url(tenant_id: str):
    """Get Google OAuth authorization URL for the tenant"""
    try:
        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()

        if not response.data:
            raise HTTPException(status_code=404, detail="OAuth settings not found for tenant")

        oauth_settings = response.data

        params = {
            'client_id': oauth_settings['client_id'],
            'redirect_uri': oauth_settings['redirect_uri'],
            'scope': ' '.join([
                'openid',
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/drive.readonly',
                'https://www.googleapis.com/auth/drive.file'
            ]),
            'response_type': 'code',
            'access_type': 'offline',
            'prompt': 'consent',
            'state': tenant_id
        }

        auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"

        return {
            "auth_url": auth_url,
            "state": tenant_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/google/status")
async def get_google_auth_status(tenant_id: str):
    """Check if tenant has valid Google OAuth tokens"""
    try:
        response = supabase.table("tenant_oauth") \
            .select("google_credentials, google_credentials_updated_at") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()

        if not response.data or not response.data.get('google_credentials'):
            return {"authenticated": False, "message": "No Google credentials found"}

        if type(response.data['google_credentials']) == str:
            token_data = json.loads(response.data['google_credentials'])
        else:
            token_data = response.data['google_credentials']

        if token_data.get('expiry'):
            expiry = datetime.fromisoformat(token_data['expiry'])
            if datetime.utcnow() > expiry:
                return {"authenticated": False, "message": "Token expired"}

        return {
            "authenticated": True,
            "tenant_id": tenant_id,
            "expires_at": token_data.get('expiry'),
            "updated_at": response.data.get('google_credentials_updated_at')
        }

    except Exception as e:
        return {"authenticated": False, "message": str(e)}


@app.post("/auth/google/save-token")
async def save_google_token(request: GoogleTokenRequest):
    """Save Google OAuth token to tenant's google_credentials column"""
    try:
        tenant_check = supabase.table("tenant_oauth") \
            .select("tenant_id") \
            .eq("tenant_id", request.tenant_id) \
            .single() \
            .execute()

        if not tenant_check.data:
            raise HTTPException(status_code=404, detail=f"Tenant OAuth settings not found for tenant {request.tenant_id}")

        token_data = {
            "access_token": request.access_token,
            "refresh_token": request.refresh_token,
            "token_type": request.token_type,
            "expires_in": request.expires_in,
            "scopes": request.scopes or [
                'https://www.googleapis.com/auth/drive.readonly',
                'https://www.googleapis.com/auth/drive.file'
            ]
        }

        if request.expires_in:
            from datetime import datetime, timedelta, timezone
            expiry = datetime.now(timezone.utc) + timedelta(seconds=request.expires_in)
            token_data["expiry"] = expiry.isoformat()

        response = supabase.table("tenant_oauth") \
            .update({
                "google_credentials": json.dumps(token_data),
                "google_credentials_updated_at": datetime.now(timezone.utc).isoformat()
            }) \
            .eq("tenant_id", request.tenant_id) \
            .execute()

        if not response.data:
            raise HTTPException(status_code=500, detail="Failed to update tenant credentials")

        return {
            "message": "Google token saved successfully",
            "tenant_id": request.tenant_id
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to save Google token: {str(e)}")


@app.post("/auth/google/callback")
async def google_oauth_callback(request: GoogleOAuthCallbackRequest):
    """Handle Google OAuth callback and exchange code for tokens"""
    try:
        tenant_id = request.state

        response = supabase.table("tenant_oauth") \
            .select("*") \
            .eq("tenant_id", tenant_id) \
            .single() \
            .execute()

        if not response.data:
            raise HTTPException(status_code=404, detail="OAuth settings not found for tenant")

        oauth_settings = response.data

        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            'client_id': oauth_settings['client_id'],
            'client_secret': oauth_settings['client_secret'],
            'code': request.code,
            'grant_type': 'authorization_code',
            'redirect_uri': oauth_settings['redirect_uri']
        }

        async with httpx.AsyncClient() as client:
            token_response = await client.post(token_url, data=token_data)

            if token_response.status_code != 200:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to exchange code for token: {token_response.text}"
                )

            token_info = token_response.json()

            if 'access_token' not in token_info:
                raise HTTPException(
                    status_code=400,
                    detail=f"Token response missing access_token: {token_info}"
                )

        token_request = GoogleTokenRequest(
            tenant_id=tenant_id,
            access_token=token_info['access_token'],
            refresh_token=token_info.get('refresh_token'),
            expires_in=token_info.get('expires_in'),
            token_type=token_info.get('token_type', 'Bearer'),
            scopes=request.scope.split(' ') if request.scope else None
        )

        await save_google_token(token_request)

        return {
            "message": "Google OAuth completed successfully",
            "tenant_id": tenant_id,
            "token_saved": True
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Retrieve / Documents
# ---------------------------------------------------------------------------

@app.get("/retrieve")
async def retrieve(
    query: str,
    tenant_id: str,
    proc_inst_id: Optional[str] = None,
    all_docs: bool = False,
    top_k: int = Query(default=5, ge=1, le=100),
    drive_folder_id: Optional[str] = None,
    room_id: Optional[str] = None,
):
    try:
        rag = get_rag_chain()
        docs = []

        if room_id:
            room_filter = {"tenant_id": tenant_id, "room_id": room_id}
            global_filter = {"tenant_id": tenant_id, "knowledge_scope": "global"}
            if drive_folder_id:
                room_filter["drive_folder_id"] = drive_folder_id
                global_filter["drive_folder_id"] = drive_folder_id

            room_result = await rag.retrieve(query, room_filter, top_k=top_k)
            global_result = await rag.retrieve(query, global_filter, top_k=top_k)
            # 글로벌 지식을 우선 병합해, 방별 문서가 많아도 글로벌 용어집이 응답 후보에서 밀리지 않도록 한다.
            raw_docs = (global_result.get("source_documents") or []) + (room_result.get("source_documents") or [])
            dedup_keys = set()
            for doc in raw_docs:
                meta = doc.metadata or {}

                dedup_key = (
                    str(meta.get("id") or ""),
                    str(meta.get("chunk_id") or ""),
                    str(meta.get("file_id") or ""),
                    str(meta.get("chunk_index") or ""),
                )
                if dedup_key in dedup_keys:
                    continue
                dedup_keys.add(dedup_key)
                docs.append(doc)
                if len(docs) >= top_k:
                    break
        elif proc_inst_id:
            metadata_filter = {
                "tenant_id": tenant_id,
                "proc_inst_id": proc_inst_id
            }
        elif all_docs:
            metadata_filter = {
                "tenant_id": tenant_id
            }
        else:
            metadata_filter = {
                "tenant_id": tenant_id,
                "source_type": "process_output"
            }

        if not room_id:
            if drive_folder_id:
                metadata_filter = {**metadata_filter, "drive_folder_id": drive_folder_id}

            result = await rag.retrieve(query, metadata_filter, top_k=top_k)
            docs = result["source_documents"]
            if drive_folder_id:
                docs = [
                    doc
                    for doc in docs
                    if (doc.metadata or {}).get("drive_folder_id") == drive_folder_id
                ]

        return {
            "response": docs
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/retrieve-images")
async def retrieve_images(
    query: str,
    tenant_id: str,
    top_k: int = Query(default=5, ge=1, le=30),
    drive_folder_id: Optional[str] = None,
):
    """캡션(Vision 분석) 기반 이미지 전용 검색 엔드포인트."""
    try:
        metadata_filter = {
            "tenant_id": tenant_id,
            "type": "image_analysis",
        }
        if drive_folder_id:
            metadata_filter["drive_folder_id"] = drive_folder_id

        rag = get_rag_chain()
        result = await rag.retrieve(query, metadata_filter, top_k=top_k)
        docs = result["source_documents"]

        if drive_folder_id:
            docs = [
                doc for doc in docs
                if (doc.metadata or {}).get("drive_folder_id") == drive_folder_id
            ]

        images = []
        for doc in docs:
            meta = doc.metadata or {}
            images.append({
                "image_id": meta.get("image_id", ""),
                "image_url": meta.get("image_url", ""),
                "caption": (doc.page_content or "").strip(),
                "file_name": meta.get("file_name", ""),
                "source_file_name": meta.get("source_file_name", ""),
                "drive_folder_name": meta.get("drive_folder_name", ""),
                "metadata": meta,
            })

        return {"images": images}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/documents/chunks-metadata")
async def get_chunks_metadata(tenant_id: str, file_name: str, drive_folder_id: Optional[str] = None):
    """특정 문서의 모든 청크 메타데이터(chunk_index, section_title, page_number 등)를 반환한다."""
    try:
        from vector_store import VectorStoreManager, get_vector_store
        vsm = get_vector_store()
        chunks = await vsm.get_all_chunks_metadata(
            tenant_id=tenant_id,
            file_name=file_name,
            drive_folder_id=drive_folder_id,
        )
        return {
            "file_name": file_name,
            "total_chunks": len(chunks),
            "chunks": chunks,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/documents/list")
async def list_documents(
    tenant_id: str,
    drive_folder_id: Optional[str] = None,
    include_images: bool = False,
):
    """특정 폴더(옵션) 내 문서명 목록을 반환한다."""
    try:
        query = (
            supabase.table("documents")
            .select("metadata")
            .eq("metadata->>tenant_id", tenant_id)
        )
        if drive_folder_id:
            query = query.eq("metadata->>drive_folder_id", drive_folder_id)
        response = query.execute()

        file_names: List[str] = []
        file_folder_map: dict = {}
        for row in response.data or []:
            meta = row.get("metadata") or {}
            if not include_images and meta.get("type") == "image_analysis":
                continue
            name = meta.get("file_name")
            if name:
                name = str(name)
                file_names.append(name)
                if name not in file_folder_map:
                    folder = meta.get("drive_folder_name") or ""
                    file_folder_map[name] = str(folder)

        unique_files = list(dict.fromkeys(file_names))
        file_details = [
            {"file_name": f, "drive_folder_name": file_folder_map.get(f, "")}
            for f in unique_files
        ]
        return {"files": unique_files, "file_details": file_details, "total": len(unique_files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/retrieve-by-indices")
async def retrieve_by_indices(request: RetrieveByIndicesRequest):
    """LLM이 선택한 chunk_index 리스트로 청크를 직접 조회한다."""
    try:
        from vector_store import VectorStoreManager, get_vector_store
        vsm = get_vector_store()
        docs = await vsm.get_chunks_by_indices(
            tenant_id=request.tenant_id,
            file_name=request.file_name,
            chunk_indices=request.chunk_indices,
            drive_folder_id=request.drive_folder_id,
        )
        return {
            "response": [
                {"page_content": doc.page_content, "metadata": doc.metadata}
                for doc in docs
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

@app.get("/query")
async def answer_query(
    query: str,
    tenant_id: str
):
    """Answer a query using the RAG system"""
    try:
        rag = get_rag_chain()

        metadata_filter = {
            "tenant_id": tenant_id
        }

        result = await rag.answer(query, metadata_filter)

        return {
            "response": result["answer"],
            "metadata": {
                f"{doc.metadata.get('file_name', 'unknown')}#{doc.metadata.get('chunk_index', i)}": doc.metadata
                for i, doc in enumerate(result["source_documents"])
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Save to Drive (upload only, no ingest)
# ---------------------------------------------------------------------------

@app.post("/save-to-drive")
async def save_to_drive(
    file: UploadFile = File(...),
    file_name: str = Form(...),
    tenant_id: str = Form(...),
    folder_path: Optional[str] = Form(None)
):
    """Save a file to Google Drive (no ingest)."""
    try:
        content = await file.read()
        file_content = io.BytesIO(content)

        drive_loader = GoogleDriveLoader(tenant_id=tenant_id)

        try:
            result = await drive_loader.save_to_google_drive(file_content, file_name, folder_path=folder_path)
            return result
        except ValueError as e:
            if "No valid Google credentials found" in str(e) or "Authentication failed" in str(e):
                auth_response = create_auth_error_response(
                    supabase,
                    tenant_id,
                    "Google Drive authentication required to upload files"
                )
                return JSONResponse(
                    status_code=401,
                    content=auth_response
                )
            else:
                raise e

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)


"""
- 문서 처리
http POST http://localhost:8005/process storage_type="drive"
http POST http://localhost:8005/process/database storage_type="database" options='{"proc_inst_id": "handover_process_definition.dae522ed-f93d-4f0c-b473-f1d79dbcf709", "activity_id": "plan_handover_schedule", "tenant_id": "localhost"}'

- 질의
http GET http://localhost:8005/query query=="프로젝트 A의 예산이 얼마 나왔지?" tenant_id==localhost

- 검색
http POST http://localhost:8005/retrieve query="교육" tenant_id="localhost"

- 산출물 처리
http POST http://localhost:8005/process-output workitem_id="bee83324-dc87-4f25-b7e2-e8e4ed5a3d8e" tenant_id="localhost"
"""
