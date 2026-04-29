"""디버그용 엔드포인트: /debug/memory."""
from __future__ import annotations

import gc
import tracemalloc
from typing import Any, Dict

from fastapi import APIRouter

from app.core.memory_monitor import read_rss_mb

router = APIRouter()


@router.get("/debug/memory")
async def debug_memory(top: int = 15, snapshot: bool = False):
    gc.collect()

    drive_jobs_info: Dict[str, Any]
    try:
        from app.services.ingest.state import drive_jobs, tenant_active_job
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
        from app.services import rag_chain as _rag_mod
        from app.services import vector_store as _vs_mod
        from app.services import document_processor as _dl_mod
        from app.storage import image_storage as _is_mod
        singletons = {
            "rag_chain": _rag_mod._rag_chain_instance is not None,
            "vector_store": _vs_mod._vector_store_instance is not None,
            "document_processor": _dl_mod._document_processor_instance is not None,
            "image_storage": _is_mod._image_storage_instance is not None,
        }
    except Exception as e:
        singletons = {"error": str(e)}

    result: Dict[str, Any] = {
        "rss_mb": read_rss_mb(),
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
