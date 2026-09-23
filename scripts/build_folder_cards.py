"""폴더 카드 일괄 백필 CLI.

대량 적재 후 1회 실행해 tenant 의 모든 폴더 카드를 bottom-up 으로 생성한다(멱등).
폴더당 LLM 1회라 문서 수와 무관하게 비용 bounded.

사용:
    python -m scripts.build_folder_cards <tenant_id>

예:
    python -m scripts.build_folder_cards acme
"""
from __future__ import annotations

import asyncio
import sys

from app.services.folder_cards import backfill_tenant


async def _main() -> int:
    if len(sys.argv) != 2:
        print("usage: python -m scripts.build_folder_cards <tenant_id>")
        return 2
    tenant_id = sys.argv[1]
    print(f"[build_folder_cards] tenant={tenant_id} 시작...")
    result = await backfill_tenant(tenant_id)
    print(f"[build_folder_cards] 완료: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
