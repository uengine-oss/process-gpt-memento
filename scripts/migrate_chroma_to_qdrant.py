"""Chroma → Qdrant 임베딩 이관.

재임베딩하지 않는다. Chroma 에 이미 들어있는 벡터·메타데이터를 그대로 읽어 Qdrant 에 넣는다.
(임베딩 API 재호출이 없으므로 폐쇄망에서도 GPU/쿼터 부담 없이 돌릴 수 있다.)

특징:
  - 배치 스트리밍 — 전체를 메모리에 올리지 않는다.
  - 재개 가능 — offset 체크포인트를 파일에 남긴다. 중단 후 같은 명령으로 이어서 실행.
  - 멱등 — point id 가 Chroma id 와 1:1 이라 재실행해도 덮어쓰기만 된다.
  - 벌크 최적화 — 적재 중 HNSW 인덱싱을 끄고(indexing_threshold=0) 끝나고 되켠다.
    수백만 벡터를 넣으면서 매번 그래프를 재구성하면 적재가 몇 배 느려진다.

사용:
    python scripts/migrate_chroma_to_qdrant.py                 # 이관 (+ 끝나고 자동 검증)
    python scripts/migrate_chroma_to_qdrant.py --verify-only   # 검증만
    python scripts/migrate_chroma_to_qdrant.py --restart       # 체크포인트 무시하고 처음부터
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Windows 콘솔 기본 코드페이지(cp949)는 진행 로그의 한글/기호를 못 찍고 죽는다.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

from app.core.env_loader import load_project_dotenv  # noqa: E402

load_project_dotenv()

from app.core import config  # noqa: E402
from app.services.vector_index import QdrantIndex, _to_point_id  # noqa: E402


DEFAULT_STATE_PATH = REPO_ROOT / ".migrate_chroma_to_qdrant.state.json"


def _chroma_collection():
    """이관 원본. VECTOR_BACKEND 값과 무관하게 항상 Chroma 를 연다."""
    host = config.chroma_server_host()
    name = config.chroma_collection_name().strip()
    if host:
        import chromadb

        client = chromadb.HttpClient(host=host, port=config.chroma_server_port())
        print(f"[migrate] 원본 Chroma: http://{host}:{config.chroma_server_port()} / {name}")
    else:
        from chromadb import PersistentClient

        persist_dir = Path(config.chroma_persist_directory()).expanduser()
        if not persist_dir.is_absolute():
            persist_dir = (REPO_ROOT / persist_dir).resolve()
        client = PersistentClient(path=str(persist_dir))
        print(f"[migrate] 원본 Chroma: {persist_dir} / {name}")
    return client.get_collection(name)


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"offset": 0, "migrated": 0}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"offset": 0, "migrated": 0}


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def _set_indexing_threshold(index: QdrantIndex, threshold: Optional[int]) -> None:
    """0 이면 HNSW 인덱싱 중단(벌크 적재용), None 이면 기본값으로 복구."""
    from qdrant_client import models

    index.client.update_collection(
        collection_name=index.collection_name,
        optimizer_config=models.OptimizersConfigDiff(
            indexing_threshold=threshold if threshold is not None else 20000
        ),
    )


def migrate(batch_size: int, state_path: Path, restart: bool) -> Dict[str, Any]:
    source = _chroma_collection()
    total = source.count()
    print(f"[migrate] Chroma 총 {total:,} 벡터")

    if total == 0:
        print("[migrate] 원본이 비어있음 — 종료")
        return {"total": 0, "migrated": 0}

    # 차원은 원본에서 읽어 확정한다 (설정값 오타로 잘못된 컬렉션을 만드는 사고 방지).
    probe = source.get(limit=1, include=["embeddings"])
    dim = len(probe["embeddings"][0])
    print(f"[migrate] 벡터 차원: {dim}")

    index = QdrantIndex()
    index.ensure_collection(vector_size=dim)

    existing = index.count()
    if existing and restart:
        print(f"[migrate] --restart: 기존 Qdrant 포인트 {existing:,} 개는 upsert 로 덮어쓴다")

    state = {"offset": 0, "migrated": 0} if restart else _load_state(state_path)
    offset = int(state.get("offset") or 0)
    migrated = int(state.get("migrated") or 0)
    if offset:
        print(f"[migrate] 체크포인트에서 재개: offset={offset:,} migrated={migrated:,}")

    print("[migrate] 벌크 적재 모드 진입 (HNSW 인덱싱 일시 중단)")
    _set_indexing_threshold(index, 0)

    t0 = time.perf_counter()
    try:
        while offset < total:
            batch = source.get(
                limit=batch_size,
                offset=offset,
                include=["embeddings", "documents", "metadatas"],
            )
            ids = list(batch.get("ids") or [])
            if not ids:
                break

            embeddings = batch.get("embeddings")
            embeddings = list(embeddings) if embeddings is not None else []
            documents = list(batch.get("documents") or [])
            metadatas = list(batch.get("metadatas") or [])

            # Chroma 는 document/metadata 가 없으면 None 을 준다 — 길이를 맞춰 zip 이 잘리지 않게.
            documents += [""] * (len(ids) - len(documents))
            metadatas += [{}] * (len(ids) - len(metadatas))
            documents = [d if d is not None else "" for d in documents]
            metadatas = [m if isinstance(m, dict) else {} for m in metadatas]

            index.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas,
                # 벌크 적재라 각 배치의 디스크 flush 를 기다리지 않는다.
                wait=False,
            )

            offset += len(ids)
            migrated += len(ids)
            _save_state(state_path, {"offset": offset, "migrated": migrated})

            elapsed = time.perf_counter() - t0
            rate = migrated / elapsed if elapsed > 0 else 0
            print(
                f"[migrate] {offset:,}/{total:,} ({offset * 100 // max(total, 1)}%) "
                f"{rate:,.0f} vec/s",
                flush=True,
            )
    finally:
        print("[migrate] HNSW 인덱싱 재개 — 그래프 구축은 백그라운드로 진행된다")
        _set_indexing_threshold(index, None)

    print(f"[migrate] 완료: {migrated:,} 벡터, {time.perf_counter() - t0:,.1f}s")
    return {"total": total, "migrated": migrated}


def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def verify(sample_size: int) -> bool:
    """개수 일치 + 표본 벡터/메타데이터 일치 확인.

    Qdrant 는 Cosine 거리라 저장 시 벡터를 L2 정규화한다. 따라서 원본과 값이 그대로 같지 않고,
    *방향*이 같다. 그래서 동일성 판정은 코사인 유사도 ≈ 1 로 한다.
    """
    import random

    source = _chroma_collection()
    index = QdrantIndex()

    src_count = source.count()
    dst_count = index.count()
    print(f"[verify] Chroma={src_count:,}  Qdrant={dst_count:,}")
    count_ok = src_count == dst_count
    if not count_ok:
        print(f"[verify] ✗ 개수 불일치 (차이 {abs(src_count - dst_count):,})")

    if src_count == 0:
        return count_ok

    n = min(sample_size, src_count)
    offsets = sorted(random.sample(range(src_count), n))
    print(f"[verify] 표본 {n} 건 대조 중...")

    vector_mismatches: List[str] = []
    missing: List[str] = []
    metadata_mismatches: List[str] = []

    for off in offsets:
        batch = source.get(limit=1, offset=off, include=["embeddings", "metadatas"])
        ids = list(batch.get("ids") or [])
        if not ids:
            continue
        rid = str(ids[0])
        src_vec = list(batch["embeddings"][0])
        src_meta = (batch.get("metadatas") or [{}])[0] or {}

        records = index.client.retrieve(
            collection_name=index.collection_name,
            ids=[_to_point_id(rid)],
            with_vectors=True,
            with_payload=True,
        )
        if not records:
            missing.append(rid)
            continue

        sim = _cosine(src_vec, list(records[0].vector))
        if sim < 0.9999:
            vector_mismatches.append(f"{rid}(cos={sim:.6f})")

        dst_meta = records[0].payload or {}
        for key in ("tenant_id", "file_id", "chunk_id", "type"):
            if key in src_meta and str(src_meta[key]) != str(dst_meta.get(key)):
                metadata_mismatches.append(
                    f"{rid}.{key}: {src_meta[key]!r} != {dst_meta.get(key)!r}"
                )

    ok = count_ok and not missing and not vector_mismatches and not metadata_mismatches
    print(f"[verify] 누락 {len(missing)} / 벡터 불일치 {len(vector_mismatches)} / "
          f"메타 불일치 {len(metadata_mismatches)}")
    for label, items in (
        ("누락", missing),
        ("벡터 불일치", vector_mismatches),
        ("메타 불일치", metadata_mismatches),
    ):
        for item in items[:5]:
            print(f"[verify]   {label}: {item}")

    print("[verify] " + ("✓ 통과" if ok else "✗ 실패"))
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Chroma → Qdrant 임베딩 이관")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("MIGRATE_BATCH_SIZE", "500")))
    parser.add_argument("--sample-size", type=int, default=50, help="검증 표본 수")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--restart", action="store_true", help="체크포인트 무시하고 처음부터")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--skip-verify", action="store_true")
    args = parser.parse_args()

    if args.verify_only:
        return 0 if verify(args.sample_size) else 1

    migrate(args.batch_size, args.state, args.restart)

    if args.skip_verify:
        return 0
    # 인덱싱 재개 직후라 세그먼트 최적화가 진행 중일 수 있다 — 개수/벡터 조회 자체는 영향 없음.
    return 0 if verify(args.sample_size) else 1


if __name__ == "__main__":
    raise SystemExit(main())
