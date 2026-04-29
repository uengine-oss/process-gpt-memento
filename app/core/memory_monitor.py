"""프로세스 메모리 사용량 주기 로깅 — startup에서 백그라운드 태스크로 기동."""
from __future__ import annotations

import asyncio
import gc
import tracemalloc
from datetime import datetime
from typing import Optional

MEMORY_LOG_INTERVAL_SEC = 60


def read_rss_mb() -> Optional[float]:
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except Exception:
        return None
    return None


def log_memory_snapshot(label: str = "periodic", top: int = 5) -> None:
    gc.collect()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rss = read_rss_mb()
    objs = len(gc.get_objects())

    jobs_total = jobs_running = 0
    try:
        from app.services.ingest.state import drive_jobs
        jobs_total = len(drive_jobs)
        jobs_running = sum(1 for j in drive_jobs.values() if j.get("status") == "running")
    except Exception:
        pass

    print(
        f"{ts} [memory:{label}] rss={rss}MB gc_objects={objs} "
        f"drive_jobs={jobs_total}(running={jobs_running})",
        flush=True,
    )

    if tracemalloc.is_tracing():
        stats = tracemalloc.take_snapshot().statistics("lineno")[:top]
        for i, s in enumerate(stats, 1):
            frame = s.traceback[0]
            print(
                f"{ts} [memory:{label}] top{i} {frame.filename}:{frame.lineno} "
                f"size={round(s.size / 1024, 1)}KB count={s.count}",
                flush=True,
            )


async def memory_log_loop() -> None:
    while True:
        await asyncio.sleep(MEMORY_LOG_INTERVAL_SEC)
        try:
            log_memory_snapshot("periodic")
        except Exception as e:
            print(f"[memory:periodic] error: {e}", flush=True)
