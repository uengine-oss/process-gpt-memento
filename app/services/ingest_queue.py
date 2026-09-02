"""지식베이스 업로드 백그라운드 인제스트 큐.

업로드 엔드포인트는 스토리지 저장 + knowledge_files `pending` 등록만 하고 즉시 반환하며,
실제 인덱싱(파싱·청킹·임베딩·OCR·요약)은 이 모듈의 *서버측 워커풀*이 처리한다.
→ 브라우저 origin당 ~6 연결 cap 과 무관하게, 외부 임베딩/LLM 서버 여력까지 동시성을 올릴 수 있다.

부하테스트가 안 된 환경이라 안전장치를 세심히 둔다:
  - 보수적 기본 동시성(MEMENTO_INGEST_CONCURRENCY, 기본 4) + 적응형 상향(AIMD, 상한 MAX).
  - transient 실패(timeout/429/5xx/424/OOM 등) → 동시한도 곱셈감소 + 쿨다운 + 파일별 지수 백오프 재시도.
  - 재시도 소진 시 failed. 영구 실패는 즉시 failed(재시도 안 함) — AIMD 는 중립(상향/페널티 없음).
  - 서킷 브레이커: 연속 실패 누적 시 풀 전체를 잠깐 정지 후 재개.
  - 멱등: 인덱싱 시작 전 기존 산출물 정리(_index_uploaded_file 내부) → 재시도/재시작에도 청크 중복 없음.
  - job 타임아웃: 한 파일이 임베딩/LLM/OCR 에서 hang 하면 워커/슬롯을 영구 점유하지 않도록 wait_for 로 강제 종료 → transient 재시도.
  - 재시작 복구 + 주기 sweeper: pending(적재 실패분 포함) + 좀비(오래된 processing) 재적재.
  - 메모리: 워커가 스토리지에서 바이트를 그때그때 받아 처리(동시 inflight 수만큼만 점유). 큐 bounded(가득 차면 pending 유지 → sweeper 복구).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional, Set

from app.core.supabase_client import supabase
from app.services.knowledge_files import (
    INDEX_STATUS_FAILED,
    INDEX_STATUS_PENDING,
    INDEX_STATUS_PROCESSING,
    mark_status,
)

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


# ── 설정(env) ────────────────────────────────────────────────────────────────
# 인제스트 전체 킬스위치. false 면 워커/복구/sweeper 를 아예 안 띄운다 → 재시작해도
# 대기/처리중 잡을 다시 집지 않는다. (대량 삭제 등으로 임베딩을 잠시 멈춰야 할 때 사용:
#  MEMENTO_INGEST_ENABLED=false 로 재시작 → 삭제 → true 로 되돌려 재시작하면 복구가 이어받음.)
_INGEST_ENABLED = _bool_env("MEMENTO_INGEST_ENABLED", True)
_BASE_CONCURRENCY = max(1, _int_env("MEMENTO_INGEST_CONCURRENCY", 4))
_MAX_CONCURRENCY = max(_BASE_CONCURRENCY, _int_env("MEMENTO_INGEST_MAX_CONCURRENCY", 12))
_MAX_RETRIES = max(0, _int_env("MEMENTO_INGEST_MAX_RETRIES", 3))
_QUEUE_MAX = max(100, _int_env("MEMENTO_INGEST_QUEUE_MAX", 20000))
_LEASE_SEC = max(120, _int_env("MEMENTO_INGEST_LEASE_SEC", 1800))       # 좀비 processing 재적재 기준
_JOB_TIMEOUT = max(60, _int_env("MEMENTO_INGEST_JOB_TIMEOUT", 900))     # 파일 1건 인덱싱 최대 시간(초). 초과 시 강제 종료→재시도
_SWEEP_INTERVAL = max(5, _int_env("MEMENTO_INGEST_SWEEP_SEC", 20))      # 복구/폴더카드 정합화 주기(초). 짧을수록 카드 재생성이 빠름(질의 가벼움)
_BACKOFF_BASE = 2.0
_BACKOFF_CAP = 60.0
_COOLDOWN_SEC = 5.0
_CB_FAIL_THRESHOLD = 6
_CB_OPEN_SEC = 30.0


def _is_transient(msg: str) -> bool:
    """일시 과부하/네트워크성 오류인지(재시도 대상) 판별."""
    m = (msg or "").lower()
    keys = (
        "timeout", "timed out", "temporarily", "overload", "rate limit", "429",
        "500", "502", "503", "504", "424", "connection", "econnreset", "reset by peer",
        "oom", "out of memory", "cuda", "too many requests", "backend error",
    )
    return any(k in m for k in keys)


class _Transient(Exception):
    pass


class _Ctrl:
    """AIMD 동시성 + 서킷브레이커 공유 상태 (단일 스레드 asyncio 라 락 불필요)."""
    def __init__(self) -> None:
        self.limit = _BASE_CONCURRENCY
        self.inflight = 0
        self.ok_streak = 0
        self.cooldown_until = 0.0
        self.fail_streak = 0
        self.open_until = 0.0

    def on_success(self) -> None:
        self.fail_streak = 0
        self.ok_streak += 1
        if self.ok_streak >= 3 and self.limit < _MAX_CONCURRENCY:
            self.limit += 1
            self.ok_streak = 0

    def on_transient(self) -> None:
        self.ok_streak = 0
        self.limit = max(1, self.limit // 2)
        self.cooldown_until = time.monotonic() + _COOLDOWN_SEC
        self.fail_streak += 1
        if self.fail_streak >= _CB_FAIL_THRESHOLD:
            self.open_until = time.monotonic() + _CB_OPEN_SEC
            logger.warning("[ingest] circuit OPEN %.0fs (fail_streak=%d)", _CB_OPEN_SEC, self.fail_streak)


_queue: "Optional[asyncio.Queue]" = None
_workers: list = []
_sweeper: Optional["asyncio.Task"] = None
_ctrl = _Ctrl()
_started = False
# 큐/처리 중인 ref 중복 방지 (재시작 복구·좀비 sweeper 가 활성 작업을 다시 넣지 않게)
_pending_refs: Set[str] = set()
# 폴더 요약 카드 재생성 대상 — (tenant, folder_path, doc_role). 파일 인덱싱 완료 시 그 폴더를 표시.
# 테넌트 인덱싱이 정착되면 sweeper 가 *그 폴더들만* rebuild_folders 로 재생성(테넌트 전체 스캔 X).
# 실제 rebuild 는 LLM(폴더당 1회)이라 오래 걸리므로 sweeper 를 막지 않게 *백그라운드 태스크*로 실행.
_dirty_folders: Set[tuple] = set()
_rebuild_in_progress: Set[str] = set()   # 테넌트별 재생성 진행 중(중복 실행 방지)
# create_task 강참조 보관 — 참조가 없으면 GC 가 재시도 태스크를 임의로 취소할 수 있어(파일 영구 정체) 반드시 유지.
_bg_tasks: Set["asyncio.Task"] = set()


def _spawn(coro) -> "asyncio.Task":
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


def _get_queue() -> "asyncio.Queue":
    global _queue
    if _queue is None:
        _queue = asyncio.Queue(maxsize=_QUEUE_MAX)
    return _queue


def enqueue_index_job(
    tenant_id: str, source_ref: str, file_name: str, doc_role: Optional[str],
    folder_path: str = "", *, attempt: int = 0,
) -> bool:
    """인덱싱 작업 적재(논블로킹). 이미 큐/처리 중이면 skip. 큐가 가득 차면 False
    (row 는 pending 으로 남고 sweeper 가 나중에 재적재)."""
    if not tenant_id or not source_ref:
        return False
    key = f"{tenant_id}::{source_ref}"
    if key in _pending_refs:
        return True
    job = {
        "tenant_id": tenant_id, "source_ref": source_ref,
        "file_name": file_name or "", "doc_role": doc_role or "content",
        "folder_path": folder_path or "", "attempt": attempt, "key": key,
    }
    try:
        _get_queue().put_nowait(job)   # 논블로킹 — 업로드 엔드포인트/복구가 큐 가득참에 안 막힘
    except asyncio.QueueFull:
        logger.warning("[ingest] queue full — %s 는 pending 으로 두고 sweeper 가 재적재", source_ref)
        return False
    _pending_refs.add(key)
    return True


async def _gate() -> None:
    """동시한도/쿨다운/서킷오픈 게이트 — 통과 가능할 때까지 대기."""
    while True:
        now = time.monotonic()
        if now < _ctrl.open_until:
            await asyncio.sleep(min(1.0, _ctrl.open_until - now))
            continue
        if now < _ctrl.cooldown_until or _ctrl.inflight >= _ctrl.limit:
            await asyncio.sleep(0.15)
            continue
        return


async def _download_bytes(source_ref: str) -> Optional[bytes]:
    try:
        data = await asyncio.to_thread(supabase.storage.from_("files").download, source_ref)
        return data if isinstance(data, (bytes, bytearray)) else None
    except Exception as e:
        logger.warning("[ingest] storage download failed (%s): %s", source_ref, e)
        return None


async def _run_job(job: Dict[str, Any]) -> str:
    """한 파일 인덱싱. 반환 'ok' | 'permanent'. transient/timeout 은 _Transient 발생."""
    tenant_id = job["tenant_id"]
    source_ref = job["source_ref"]

    await mark_status(tenant_id, "upload", source_ref, INDEX_STATUS_PROCESSING)

    content = await _download_bytes(source_ref)
    if content is None:
        raise _Transient("storage download returned empty")

    # _index_uploaded_file: 멱등 정리 후 풀 파이프라인. 성공 시 indexed 마킹, 오류는 문자열 반환(자체 failed 마킹은 제거됨).
    from app.api.knowledge_admin import _index_uploaded_file  # 지연 import (순환 방지)
    try:
        err = await asyncio.wait_for(
            _index_uploaded_file(
                tenant_id=tenant_id,
                storage_path=source_ref,
                file_content=content,
                file_name=job["file_name"],
                doc_role=job["doc_role"],
                public_url=None,
            ),
            timeout=_JOB_TIMEOUT,
        )
    except asyncio.TimeoutError:
        # 한 파일이 hang → 강제 종료하고 transient 로 재시도 (워커/슬롯 회수)
        raise _Transient(f"job timeout ({_JOB_TIMEOUT}s)")

    if err:
        if _is_transient(err):
            raise _Transient(err)
        # 영구 실패 — terminal 은 큐가 소유
        await mark_status(tenant_id, "upload", source_ref, INDEX_STATUS_FAILED, error=str(err)[:300])
        logger.info("[ingest] permanent failure %s: %s", source_ref, str(err)[:200])
        return "permanent"
    return "ok"


async def _requeue_after(job: Dict[str, Any], delay: float) -> None:
    await mark_status(job["tenant_id"], "upload", job["source_ref"], INDEX_STATUS_PENDING)
    await asyncio.sleep(delay)
    _pending_refs.discard(job["key"])           # enqueue 가 다시 추가
    enqueue_index_job(
        job["tenant_id"], job["source_ref"], job["file_name"], job["doc_role"],
        job.get("folder_path", ""), attempt=job["attempt"] + 1,
    )


async def _worker(idx: int) -> None:
    q = _get_queue()
    while True:
        job = await q.get()
        try:
            await _gate()
            _ctrl.inflight += 1
            try:
                outcome = await _run_job(job)
            finally:
                _ctrl.inflight -= 1
            if outcome == "ok":
                _ctrl.on_success()
                # 인덱싱 완료 → 그 파일이 속한 폴더를 카드 재생성 대상으로 표시(루트 '' 는 폴더카드 없음)
                _fp = (job.get("folder_path") or "").strip().strip("/")
                if _fp:
                    _dirty_folders.add((job["tenant_id"], _fp, job.get("doc_role") or "content"))
            # 'permanent' → AIMD 중립(상향/페널티 없음): 영구 실패로 동시성을 올리거나 서킷을 건드리지 않음
            _pending_refs.discard(job["key"])
        except _Transient as t:
            _ctrl.on_transient()
            if job["attempt"] < _MAX_RETRIES:
                delay = min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** job["attempt"]))
                logger.warning(
                    "[ingest] transient %s (attempt %d/%d) → retry in %.0fs: %s",
                    job["source_ref"], job["attempt"] + 1, _MAX_RETRIES, delay, str(t)[:160],
                )
                _spawn(_requeue_after(job, delay))   # 강참조 유지 필수(GC 유실 방지)
            else:
                logger.error("[ingest] giving up %s after %d retries: %s",
                             job["source_ref"], _MAX_RETRIES, str(t)[:160])
                await mark_status(job["tenant_id"], "upload", job["source_ref"],
                                  INDEX_STATUS_FAILED, error=f"max retries: {str(t)[:280]}")
                _pending_refs.discard(job["key"])
        except Exception as e:
            logger.exception("[ingest] worker %d unexpected error: %s", idx, e)
            try:
                await mark_status(job["tenant_id"], "upload", job["source_ref"],
                                  INDEX_STATUS_FAILED, error=str(e)[:300])
            except Exception:
                pass
            _pending_refs.discard(job["key"])
        finally:
            q.task_done()


def _age_seconds(iso: Optional[str]) -> Optional[float]:
    if not iso:
        return None
    try:
        from datetime import datetime, timezone
        s = str(iso).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


async def _recover_stuck(initial: bool = False) -> int:
    """pending/processing(좀비) upload 행을 재적재.

    - pending: 항상 재적재(큐 가득참으로 적재 실패했거나 재시작 전 남은 것). 이미 큐/처리 중이면 dedup 로 skip.
    - processing: initial 이면 전부, 주기 호출이면 LEASE 보다 오래된 좀비만.
    """
    try:
        q = (
            supabase.table("knowledge_files")
            .select("tenant_id, source_ref, file_name, doc_role, folder_path, index_status, updated_at")
            .eq("source_type", "upload")
            .in_("index_status", [INDEX_STATUS_PENDING, INDEX_STATUS_PROCESSING])
        )
        rows = (await asyncio.to_thread(q.limit(50000).execute)).data or []
    except Exception as e:
        logger.warning("[ingest] recover query failed: %s", e)
        return 0

    n = 0
    for r in rows:
        ref = r.get("source_ref")
        if not ref:
            continue
        _fp = (r.get("folder_path") or "").strip().strip("/")
        # 재시작 갭 보정: 진행 중이던 폴더를 카드 재생성 대상으로 seed
        #  → 그 테넌트가 정착되면 sweeper 가 그 폴더 카드를 재생성(재시작 직전 완료분도 반영).
        if initial and _fp:
            _dirty_folders.add((r["tenant_id"], _fp, r.get("doc_role") or "content"))
        if not initial and r.get("index_status") == INDEX_STATUS_PROCESSING:
            # 좀비 판정: updated_at 이 LEASE 보다 오래됐을 때만(진행 중인 건 건드리지 않음).
            # 활성 작업은 _pending_refs 에 있어 enqueue 가 어차피 skip 하지만, 불필요한 시도도 줄인다.
            ts = _age_seconds(r.get("updated_at"))
            if ts is None or ts < _LEASE_SEC:
                continue
        # 이미 큐/처리 중(_pending_refs)인 건 dedup-skip 되므로 *새로 적재된 것만* 카운트(로그 정확도).
        was_new = f"{r['tenant_id']}::{ref}" not in _pending_refs
        if enqueue_index_job(r["tenant_id"], ref, r.get("file_name") or "", r.get("doc_role"), _fp) and was_new:
            n += 1
    if n:
        logger.info("[ingest] recovered/re-enqueued %d job(s) (initial=%s)", n, initial)
    return n


async def _tenant_has_active(tenant_id: str) -> bool:
    """그 테넌트에 아직 pending/processing 인 upload 파일이 있나(=인덱싱 미정착)."""
    try:
        rows = (await asyncio.to_thread(
            supabase.table("knowledge_files").select("source_ref")
            .eq("tenant_id", tenant_id).eq("source_type", "upload")
            .in_("index_status", [INDEX_STATUS_PENDING, INDEX_STATUS_PROCESSING])
            .limit(1).execute
        )).data or []
        return len(rows) > 0
    except Exception:
        return True   # 불확실하면 아직 활성으로 간주(다음 tick 재시도)


async def _do_rebuild_cards(tenant: str, folders_by_role: Dict[str, list]) -> None:
    """실제 폴더 카드 재생성 — *백그라운드 태스크*로 실행(폴더당 LLM 이라 오래 걸림, sweeper 안 막음)."""
    try:
        from app.services.folder_cards import rebuild_folders
        total = 0
        for role, paths in folders_by_role.items():
            if paths:
                await rebuild_folders(tenant, paths, role)   # 지정 폴더 + 조상만, bottom-up, signature-skip
                total += len(paths)
        logger.info("[ingest] folder cards reconciled: tenant=%s folders=%d", tenant, total)
    except Exception as e:
        logger.warning("[ingest] folder card rebuild failed (%s): %s", tenant, e)
        for role, paths in folders_by_role.items():   # 실패분 재적재 → 다음 tick 재시도
            for fp in paths:
                _dirty_folders.add((tenant, fp, role))
    finally:
        _rebuild_in_progress.discard(tenant)


async def _rebuild_settled_folder_cards() -> None:
    """인덱싱이 정착된 테넌트의 *dirty 폴더만* 카드 재생성(서버 주도, 프론트 무관).

    - 테넌트 인덱싱 정착(pending/processing 0) 후에만 → 파일이 계속 들어오는 폴더 조기 재생성 방지.
    - 실제 재생성은 백그라운드 태스크로 → 폴더당 LLM 이라 오래 걸려도 sweeper/복구를 막지 않음.
    - 테넌트 전체가 아니라 바뀐 폴더 + 조상만(rebuild_folders) → 대형 테넌트 전체 스캔 회피.
    """
    if not _dirty_folders:
        return
    tenants = {t for (t, _fp, _r) in _dirty_folders}
    for tenant in tenants:
        if tenant in _rebuild_in_progress:
            continue
        if await _tenant_has_active(tenant):
            continue   # 아직 인덱싱 중 → 정착 후 다음 tick 에
        items = [(fp, role) for (t, fp, role) in _dirty_folders if t == tenant]
        for fp, role in items:
            _dirty_folders.discard((tenant, fp, role))
        by_role: Dict[str, list] = {}
        for fp, role in items:
            by_role.setdefault(role or "content", []).append(fp)
        _rebuild_in_progress.add(tenant)
        _spawn(_do_rebuild_cards(tenant, by_role))   # 논블로킹


async def _sweeper_loop() -> None:
    interval = float(_SWEEP_INTERVAL)
    while True:
        try:
            await asyncio.sleep(interval)
            await _recover_stuck(initial=False)
            await _rebuild_settled_folder_cards()   # 서버 주도 폴더 카드 정합화
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[ingest] sweeper error: %s", e)


async def start_ingest_workers() -> None:
    """startup 에서 1회 호출 — 워커풀 기동 + 재시작 복구 + 좀비/미적재 sweeper."""
    global _started, _sweeper
    if _started:
        return
    _started = True
    if not _INGEST_ENABLED:
        # 킬스위치 ON — 워커/복구/sweeper 미기동. 큐에 잡이 들어와도(enqueue) 처리하지 않는다.
        logger.warning(
            "[ingest] DISABLED via MEMENTO_INGEST_ENABLED=false — workers/recovery/sweeper not started"
        )
        return
    _get_queue()
    for i in range(_MAX_CONCURRENCY):
        _workers.append(_spawn(_worker(i)))
    logger.info(
        "[ingest] workers started: max=%d base_limit=%d retries=%d lease=%ds job_timeout=%ds",
        _MAX_CONCURRENCY, _BASE_CONCURRENCY, _MAX_RETRIES, _LEASE_SEC, _JOB_TIMEOUT,
    )
    await _recover_stuck(initial=True)   # enqueue 논블로킹이라 startup 을 막지 않음
    _sweeper = _spawn(_sweeper_loop())


async def ingest_status_counts(tenant_id: str) -> Dict[str, Any]:
    """관측성: 테넌트 upload 파일의 상태별 카운트 + 큐/동시성 스냅샷."""
    counts: Dict[str, int] = {}
    try:
        rows = (await asyncio.to_thread(
            supabase.table("knowledge_files").select("index_status")
            .eq("tenant_id", tenant_id).eq("source_type", "upload")
            .limit(100000).execute
        )).data or []
        for r in rows:
            s = r.get("index_status") or "unknown"
            counts[s] = counts.get(s, 0) + 1
    except Exception as e:
        logger.warning("[ingest] status counts failed: %s", e)
    return {
        "counts": counts,
        "queue_size": _get_queue().qsize() if _queue else 0,
        "inflight": _ctrl.inflight,
        "limit": _ctrl.limit,
        "max": _MAX_CONCURRENCY,
        "circuit_open": time.monotonic() < _ctrl.open_until,
    }
