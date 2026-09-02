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
from app.api.folders import router as folders_router
from app.api.ingest import router as ingest_router
from app.api.knowledge_admin import router as knowledge_admin_router
from app.api.legal_review import router as legal_review_router
from app.api.navigator import router as navigator_router
from app.api.parse_preview import router as parse_preview_router
from app.api.query import router as query_router
from app.api.retrieve import router as retrieve_router
from app.api.summary import router as summary_router
from app.core.logging_setup import attach_to_uvicorn_loggers
from app.core.memory_monitor import log_memory_snapshot, memory_log_loop
from app.plugins.chunkers import log_active_strategy as log_chunker_strategy
from app.plugins.parsers import log_active_strategy as log_parser_strategy
from app.plugins.retrievers import log_active_strategy as log_retriever_strategy
from app.services.llm import log_provider_config
from app.services.rag_chain import get_rag_chain
from app.services.vector_store import get_vector_store


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
app.include_router(knowledge_admin_router)
app.include_router(legal_review_router)
app.include_router(navigator_router)
app.include_router(parse_preview_router)
app.include_router(folders_router)
app.include_router(summary_router)


@app.on_event("startup")
async def _log_startup_config():
    # asyncio 기본 스레드풀 확대 — 임베딩/LLM/Supabase/Chroma 호출을 to_thread 로 오프로드하는데,
    # 기본 풀(min(32, cpu+4))이 작아서 동시 업로드가 스레드를 다 점유하면 폴더 조회 등 다른
    # 요청의 to_thread 가 스레드를 못 잡아 느려진다(CPU 는 거의 I/O 대기라 놀고 있음).
    # 이 워크로드는 I/O 바운드라 풀을 코어 수보다 크게 잡아도 CPU 부담이 없다.
    # MEMENTO_THREAD_POOL 로 조절(기본 64, 0/음수면 비활성).
    try:
        from concurrent.futures import ThreadPoolExecutor
        _pool = int(os.getenv("MEMENTO_THREAD_POOL", "64"))
        if _pool > 0:
            asyncio.get_running_loop().set_default_executor(
                ThreadPoolExecutor(max_workers=_pool, thread_name_prefix="memento-io")
            )
            print(f"[startup] asyncio default thread pool max_workers={_pool}", flush=True)
    except Exception as e:
        print(f"[startup] thread pool resize skipped: {e}", flush=True)

    attach_to_uvicorn_loggers()
    log_provider_config()
    log_chunker_strategy()
    log_retriever_strategy()
    if os.getenv("MEMENTO_TRACEMALLOC", "0").strip().lower() in {"1", "true", "yes", "on"}:
        if not tracemalloc.is_tracing():
            tracemalloc.start(25)
            print("tracemalloc started (depth=25)", flush=True)
    log_memory_snapshot("startup")

    # 서버 시작 시 싱글턴 인스턴스를 즉시 초기화 (lazy 로딩 방지 및 race condition 제거)
    print("Pre-initializing VectorStore and RAGChain...", flush=True)
    await asyncio.to_thread(get_vector_store)
    await asyncio.to_thread(get_rag_chain)
    print("Pre-initialization complete.", flush=True)

    # 지식베이스 백그라운드 인제스트 워커풀 기동 + 재시작 복구(pending/processing 재적재).
    try:
        from app.services.ingest_queue import start_ingest_workers
        await start_ingest_workers()
    except Exception as e:
        print(f"[startup] ingest workers start failed: {e}", flush=True)

    asyncio.create_task(memory_log_loop())
