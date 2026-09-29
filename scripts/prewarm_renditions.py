"""인용 뷰어 변환본 일괄 백필 CLI.

인제스트 직후 미리 변환(RENDITION_PREWARM)이 생기기 전에 올라온 HWPX·DOCX 는 첫 열람이 변환을
기다린다(HWPX 수십 초). 이미 로컬 캐시나 비공개 버킷에 있으면 건너뛰므로 여러 번 돌려도 된다.

사용:
    python -m scripts.prewarm_renditions <tenant_id> [--folder 접두어] [--limit N] [--dry-run]

예:
    python -m scripts.prewarm_renditions localhost --folder 출처시각화
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from typing import Any, Dict, List, Optional

from app.api.citations import _all_blocks
from app.core.supabase_client import supabase
from app.services import rendition
from app.storage.artifact_bucket import bucket_for


async def _files(tenant_id: str, folder: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    while True:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files").select("source_ref, file_name, folder_path, file_hash, index_status")
            .eq("tenant_id", tenant_id).range(len(out), len(out) + 999).execute
        )
        batch = result.data or []
        out.extend(batch)
        if len(batch) < 1000:
            break
    prefix = (folder or "").strip("/")
    return [
        item for item in out
        if item.get("source_ref") and rendition.renderer_for(item.get("file_name") or "")
        and item.get("index_status") == "indexed"
        and (not prefix or (item.get("folder_path") or "").strip("/") == prefix
             or (item.get("folder_path") or "").startswith(prefix + "/"))
    ]


async def _one(tenant_id: str, item: Dict[str, Any]) -> str:
    file_id = str(item["source_ref"])
    blocks = await _all_blocks(tenant_id, file_id)
    if not blocks:
        return "no-blocks"
    rend = await rendition.ensure_rendition(
        file_id=file_id, file_name=item.get("file_name") or file_id, file_hash=item.get("file_hash") or "",
        blocks=blocks,
        load_bytes=lambda: asyncio.to_thread(supabase.storage.from_(bucket_for(file_id)).download, file_id),
    )
    if not rend:
        return "no-renderer"
    cov = rend.get("coverage") or {}
    return f"{rend['page_count']}쪽, 블록 {cov.get('placed')}/{cov.get('blocks')}"


async def _main() -> int:
    parser = argparse.ArgumentParser(description="인용 뷰어 변환본 백필")
    parser.add_argument("tenant_id")
    parser.add_argument("--folder", default="", help="이 폴더 접두어 아래만")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    files = await _files(args.tenant_id, args.folder or None)
    if args.limit:
        files = files[: args.limit]
    print(f"대상 {len(files)}건", flush=True)
    failed = 0
    for n, item in enumerate(files, 1):
        name = f"{item.get('folder_path') or ''}/{item.get('file_name')}"
        if args.dry_run:
            print(f"[{n}/{len(files)}] {name}", flush=True)
            continue
        started = time.perf_counter()
        try:
            status = await _one(args.tenant_id, item)
        except Exception as exc:  # noqa: BLE001 - 한 건 실패로 멈추지 않는다
            failed += 1
            status = f"실패: {type(exc).__name__}: {str(exc)[:200]}"
        print(f"[{n}/{len(files)}] {name} — {status} ({time.perf_counter() - started:.1f}초)", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
