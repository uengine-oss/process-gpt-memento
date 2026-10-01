"""옛 파서로 인덱싱된 업로드 파일을 재인덱싱 대기(pending)로 돌린다.

`knowledge_files.parser_version` 이 지금 파서 버전(`parser_version()`, 표 LLM 을 켜면 모델이 붙는다)과 다른(또는 비어 있는)
indexed 파일이 대상이다. 표 LLM 을 켜고 끈 뒤에는 `--ext pdf` 로 PDF 만 돌린다(다른 형식은 결과가 같다).
실제 재처리는 돌고 있는 memento 서버의 인제스트 sweeper 가 pending 을 다시 적재해서 한다(멱등: 옛 페이지·블록·
카드·벡터를 먼저 지운다). 그림 설명·카드 LLM 을 다시 부르므로 폴더·형식을 좁혀 나눠 돌린다.

사용:
    python -m scripts.reindex_stale <tenant_id> [--folder 접두어] [--ext pdf,hwp,hwpx,docx] [--limit N] [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.supabase_client import supabase
from app.plugins.parsers import parser_version
from app.services.knowledge_files import INDEX_STATUS_INDEXED, INDEX_STATUS_PENDING, mark_status


async def _stale(tenant_id: str, folder: Optional[str], exts: Optional[set]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    while True:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_type, source_ref, file_name, folder_path, index_status, parser_version")
            .eq("tenant_id", tenant_id).eq("source_type", "upload")
            .range(len(rows), len(rows) + 999).execute
        )
        batch = result.data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
    prefix = (folder or "").strip("/")
    current = parser_version()
    return [
        r for r in rows
        if r.get("index_status") == INDEX_STATUS_INDEXED
        and r.get("parser_version") != current
        and (not exts or Path(r.get("file_name") or "").suffix.lower().lstrip(".") in exts)
        and (not prefix or (r.get("folder_path") or "").strip("/") == prefix
             or (r.get("folder_path") or "").startswith(prefix + "/"))
    ]


async def _main() -> int:
    parser = argparse.ArgumentParser(description="옛 파서 버전 파일을 재인덱싱 대기로")
    parser.add_argument("tenant_id")
    parser.add_argument("--folder", default="", help="이 폴더 접두어 아래만")
    parser.add_argument("--ext", default="", help="쉼표로 구분한 확장자만 (예: pdf,hwp)")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    exts = {e.strip().lower().lstrip(".") for e in args.ext.split(",") if e.strip()} or None
    current = parser_version()
    rows = await _stale(args.tenant_id, args.folder or None, exts)
    if args.limit:
        rows = rows[: args.limit]
    by_ext = Counter(Path(r.get("file_name") or "").suffix.lower() for r in rows)
    by_ver = Counter(r.get("parser_version") or "(없음)" for r in rows)
    print(f"대상 {len(rows)}건 (지금 파서 {current}) 형식 {dict(by_ext)} 옛 버전 {dict(by_ver)}", flush=True)
    if args.dry_run:
        for r in rows:
            print(f"  {r.get('folder_path') or ''}/{r.get('file_name')}  [{r.get('parser_version')}]")
        return 0
    for r in rows:
        await mark_status(tenant_id=args.tenant_id, source_type="upload", source_ref=r["source_ref"],
                          status=INDEX_STATUS_PENDING)
    print("pending 으로 돌렸다. 서버 인제스트 sweeper 가 차례로 다시 인덱싱한다(`/knowledge/ingest/status`).")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
