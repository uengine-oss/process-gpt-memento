"""Drive 폴더 인덱싱 작업 상태 (in-memory) — 라우터/서비스 공통."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List

drive_jobs: Dict[str, dict] = {}
tenant_active_job: Dict[str, str] = {}

DRIVE_JOB_TTL_SECONDS = 3600
DRIVE_JOB_MAX_ENTRIES = 50

SUPPORTED_MIME_TYPES: List[str] = [
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/vnd.google-apps.presentation",
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/x-hwp",
    "application/haansofthwp",
    "application/haansofthwpx",
    "application/haansoftpdf",
    "application/haansoftdocx",
    "application/vnd.hancom.hwp",
    "application/vnd.hancom.hwpx",
    "application/octet-stream",  # Drive가 hwp/hwpx 등 미인식 파일에 부여하는 타입
    "text/plain",
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/bmp",
    "image/webp",
]

IMAGE_MIME_TYPES: List[str] = [
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/bmp",
    "image/webp",
]


def cleanup_drive_jobs() -> None:
    """완료/실패 후 TTL을 넘긴 잡과 한도 초과 잡을 정리."""
    try:
        now = datetime.now()
        for jid in [
            jid for jid, job in list(drive_jobs.items())
            if job.get("status") != "running"
            and job.get("finished_at")
            and (now - datetime.fromisoformat(job["finished_at"])).total_seconds()
            > DRIVE_JOB_TTL_SECONDS
        ]:
            drive_jobs.pop(jid, None)

        completed = [
            (jid, job) for jid, job in drive_jobs.items()
            if job.get("status") != "running"
        ]
        if len(completed) > DRIVE_JOB_MAX_ENTRIES:
            completed.sort(
                key=lambda kv: kv[1].get("finished_at") or kv[1].get("created_at") or ""
            )
            for jid, _ in completed[: len(completed) - DRIVE_JOB_MAX_ENTRIES]:
                drive_jobs.pop(jid, None)
    except Exception as e:
        print(f"cleanup_drive_jobs error: {e}")
