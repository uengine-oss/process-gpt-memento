"""FastAPI 앱 — 미들웨어/스타트업/라우터 등록만 담당."""
from __future__ import annotations

import asyncio
import os
import time
import tracemalloc

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.api.auth import router as auth_router
from app.api.debug import router as debug_router
from app.api.ingest import router as ingest_router
from app.api.query import router as query_router
from app.api.retrieve import router as retrieve_router
from app.core.logging_setup import attach_to_uvicorn_loggers
from app.core.memory_monitor import log_memory_snapshot, memory_log_loop
from app.plugins.chunkers import log_active_strategy as log_chunker_strategy
from app.plugins.parsers import log_active_strategy as log_parser_strategy
from app.plugins.retrievers import log_active_strategy as log_retriever_strategy
from app.services.llm import log_provider_config


app = FastAPI(title="Memento Service API", description="API for document processing and querying")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def _request_log(request: Request, call_next):
    """모든 요청 진입/완료 로깅 — method, path, query, tenant_id, 소요시간."""
    started = time.perf_counter()
    qs = str(request.url.query) if request.url.query else ""
    tenant = request.query_params.get("tenant_id") or ""
    print(f"[http] -> {request.method} {request.url.path} tenant={tenant!r} qs={qs[:200]!r}")
    try:
        response = await call_next(request)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        print(f"[http] <- {request.method} {request.url.path} status={response.status_code} {elapsed_ms}ms")
        return response
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        print(f"[http] !! {request.method} {request.url.path} error={e!r} {elapsed_ms}ms")
        raise


app.include_router(auth_router)
app.include_router(retrieve_router)
app.include_router(query_router)
app.include_router(debug_router)
app.include_router(ingest_router)


@app.on_event("startup")
async def _log_startup_config():
    attach_to_uvicorn_loggers()
    log_provider_config()
    log_chunker_strategy()
    log_retriever_strategy()
    if os.getenv("MEMENTO_TRACEMALLOC", "0").strip().lower() in {"1", "true", "yes", "on"}:
        if not tracemalloc.is_tracing():
            tracemalloc.start(25)
            print("tracemalloc started (depth=25)", flush=True)
    log_memory_snapshot("startup")
    asyncio.create_task(memory_log_loop())
