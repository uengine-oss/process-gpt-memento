"""문서 카드 일괄 백필 CLI.

이미 인제스트된 문서의 페이지 본문(``document_pages``)만 가지고 카드를 다시 만든다.
파싱·임베딩을 다시 하지 않으므로 재업로드보다 훨씬 싸고, 본문이 이미 검증된 상태라
카드만 실패한 경우(모델 타임아웃 등)의 정상 복구 경로다.

기본은 *비어 있거나 실패한 카드만* 다시 만든다. 내용이 있는 카드는 건드리지 않는다.

사용:
    python -m scripts.build_doc_cards <tenant_id> [--folder 접두어] [--all] [--limit N]
                                      [--concurrency N] [--dry-run]

예:
    python -m scripts.build_doc_cards localhost --folder 법제처_300
    python -m scripts.build_doc_cards localhost --all --limit 50
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from typing import Any, Dict, List, Optional

from app.core.supabase_client import supabase
from app.services import doc_cards


def _page_marker_text(rows: List[Dict[str, Any]]) -> str:
    blocks = []
    for row in sorted(rows, key=lambda item: int(item.get("page_number") or 0)):
        content = str(row.get("content") or "").strip()
        if content:
            blocks.append(f"=== page {int(row.get('page_number') or 0)} ===\n{content}")
    return "\n\n".join(blocks)


async def _rows(table: str, params: Dict[str, Any], select: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        query = supabase.table(table).select(select)
        for key, value in params.items():
            query = query.eq(key, value)
        result = await asyncio.to_thread(query.range(offset, offset + 999).execute)
        batch = getattr(result, "data", None) or []
        out.extend(batch)
        if len(batch) < 1000:
            return out
        offset += 1000


async def _files(tenant_id: str, folder: Optional[str]) -> List[Dict[str, Any]]:
    files = await _rows(
        "knowledge_files", {"tenant_id": tenant_id}, "source_ref, file_name, folder_path"
    )
    if folder:
        prefix = folder.strip("/")
        files = [
            item for item in files
            if (item.get("folder_path") or "").strip("/") == prefix
            or (item.get("folder_path") or "").startswith(prefix + "/")
        ]
    return [item for item in files if item.get("source_ref")]


async def _existing_cards(tenant_id: str) -> Dict[str, Dict[str, Any]]:
    rows = await _rows("knowledge_doc_cards", {"tenant_id": tenant_id}, "file_id, card, status")
    return {str(row["file_id"]): row for row in rows if row.get("file_id")}


def _needs_card(row: Optional[Dict[str, Any]]) -> bool:
    """내용이 없는 카드는 카드가 아니다 — 실패했거나 아직 안 만들어진 것."""
    if row is None:
        return True
    if str(row.get("status") or "") != "done":
        return True
    card = row.get("card") if isinstance(row.get("card"), dict) else {}
    return not (card.get("summary") or card.get("answers_questions") or card.get("topics"))


async def _build_one(item: Dict[str, Any], tenant_id: str, dry_run: bool) -> str:
    file_id = str(item["source_ref"])
    file_name = str(item.get("file_name") or file_id)
    pages = await _rows(
        "document_pages", {"tenant_id": tenant_id, "file_id": file_id}, "page_number, content"
    )
    text = _page_marker_text(pages)
    if not text.strip():
        return "no-text"
    if dry_run:
        return "would-build"

    async with doc_cards.card_gate():
        card = await doc_cards.build_card(file_name=file_name, text=text)
    failed = (
        card.coverage.windows_read > 0
        and card.coverage.windows_failed >= card.coverage.windows_read
    )
    await doc_cards.save_card(
        tenant_id=tenant_id, file_id=file_id, card=card.as_dict(),
        signature=doc_cards.card_signature(text=text, model=""),
        content_hash=doc_cards.content_sha256(text),
        status="failed" if failed else "done",
    )
    await doc_cards.save_text_stats(
        tenant_id=tenant_id, file_id=file_id, text=text, page_count=len(pages)
    )
    return "failed" if failed else "done"


async def _main() -> int:
    parser = argparse.ArgumentParser(description="문서 카드 백필")
    parser.add_argument("tenant_id")
    parser.add_argument("--folder", default="", help="이 폴더 접두어 아래만")
    parser.add_argument("--all", action="store_true", help="내용 있는 카드까지 전부 다시 만든다")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=doc_cards.CARD_CONCURRENCY)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    doc_cards.CARD_CONCURRENCY = max(1, args.concurrency)
    doc_cards._card_gate = None  # noqa: SLF001 - CLI 에서 동시성을 정하고 시작한다

    files = await _files(args.tenant_id, args.folder or None)
    cards = await _existing_cards(args.tenant_id)
    targets = [item for item in files if args.all or _needs_card(cards.get(str(item["source_ref"])))]
    if args.limit:
        targets = targets[: args.limit]

    print(f"[build_doc_cards] tenant={args.tenant_id} folder={args.folder or '(전체)'} "
          f"대상 {len(targets)}건 / 스코프 {len(files)}건 · 동시 {doc_cards.CARD_CONCURRENCY}")
    if not targets:
        print("[build_doc_cards] 다시 만들 카드가 없다.")
        return 0

    started = time.perf_counter()
    counts: Dict[str, int] = {}
    done = 0

    async def run(item: Dict[str, Any]) -> None:
        nonlocal done
        try:
            outcome = await _build_one(item, args.tenant_id, args.dry_run)
        except Exception as exc:  # noqa: BLE001 - 한 건 실패가 백필을 멈추지 않는다
            outcome = "error"
            print(f"  ! {item.get('file_name')}: {exc}")
        counts[outcome] = counts.get(outcome, 0) + 1
        done += 1
        if done % 10 == 0 or done == len(targets):
            elapsed = time.perf_counter() - started
            print(f"  {done}/{len(targets)} · {elapsed:.0f}초 · {counts}")

    await asyncio.gather(*(run(item) for item in targets))
    print(f"[build_doc_cards] 완료: {counts} · {time.perf_counter() - started:.0f}초")
    return 0 if not counts.get("error") else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
