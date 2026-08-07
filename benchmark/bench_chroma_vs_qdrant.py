"""Chroma vs Qdrant — 상주 메모리 / 검색 지연 실측 벤치마크.

memento 실데이터가 아니라 *동일 규모의 합성 벡터*로 두 엔진만 격리 비교한다.
(실 API 응답시간은 임베딩 호출이 지배해 엔진 차이가 묻힌다. 여기서는 미리 만든 쿼리
벡터를 그대로 던져 엔진+네트워크 왕복만 잰다.)

측정 항목
  - anon    : 프로세스가 붙잡고 있는 익명 메모리. **반납 불가** — 실질 RAM 점유.
  - file    : 파일 기반 페이지 캐시(mmap). 메모리 압박 시 커널이 회수 가능.
  - current : cgroup 총 사용량(anon + file + 기타).
  Chroma 는 인덱스를 힙에 올리므로 anon 이 곧 점유다. Qdrant 는 양자화본만 anon 이고
  원본 벡터/HNSW 는 file 이라, 압박이 오면 file 쪽이 회수된다 — 이게 핵심 차이다.

usage:
    python benchmark/bench_chroma_vs_qdrant.py --target 200000 --checkpoints 50000,100000,200000
"""
from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8", errors="replace")

DIM = 1536  # memento 실사용 차원과 동일하게
CHROMA_NAME = "bench-chroma"
QDRANT_NAME = "bench-qdrant"
CHROMA_PORT = 8003
QDRANT_PORT = 6343
CHROMA_IMAGE = "chromadb/chroma:1.5.9"
QDRANT_IMAGE = "qdrant/qdrant:v1.19.0"
COLLECTION = "documents"


def sh(cmd: List[str], check: bool = True) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n{proc.stderr}")
    return proc.stdout.strip()


# ── 컨테이너 ──


def container_up(name: str, image: str, port: int, internal: int, volume: str, cmd: List[str]):
    sh(["docker", "rm", "-f", name], check=False)
    sh(["docker", "volume", "rm", volume], check=False)
    sh(["docker", "volume", "create", volume])
    args = ["docker", "run", "-d", "--name", name, "-p", f"{port}:{internal}"]
    if name == CHROMA_NAME:
        args += ["-v", f"{volume}:/data"]
    else:
        # 운영 compose 와 동일 — 기본 32MB 상한은 대량 업서트 배치에서 바로 걸린다.
        args += ["-v", f"{volume}:/qdrant/storage",
                 "-e", "QDRANT__SERVICE__MAX_REQUEST_SIZE_MB=256"]
    args += [image] + cmd
    sh(args)


def container_restart(name: str) -> None:
    """재시작 후 메모리를 재려면 캐시가 비워진 상태에서 다시 채워지게 해야 한다."""
    sh(["docker", "restart", name])


def _read_kv(name: str, path: str) -> Dict[str, float]:
    out = sh(["docker", "exec", name, "cat", path], check=False)
    stats: Dict[str, float] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                stats[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return stats


def mem_stats(name: str) -> Dict[str, float]:
    """컨테이너 메모리 내역(MB). cgroup v2/v1 모두 지원.

    anon = 힙 등 익명 메모리(회수 불가, 실질 점유).
    file = 파일 기반 페이지 캐시/mmap(메모리 압박 시 커널이 회수).
    """
    mb = 1024 * 1024

    v2 = _read_kv(name, "/sys/fs/cgroup/memory.stat")
    if "anon" in v2:
        cur = sh(["docker", "exec", name, "cat", "/sys/fs/cgroup/memory.current"], check=False)
        return {
            "anon_mb": v2.get("anon", 0) / mb,
            "file_mb": v2.get("file", 0) / mb,
            "current_mb": (float(cur) if cur.strip().isdigit() else 0.0) / mb,
            "cgroup": 2,
        }

    v1 = _read_kv(name, "/sys/fs/cgroup/memory/memory.stat")
    cur = sh(
        ["docker", "exec", name, "cat", "/sys/fs/cgroup/memory/memory.usage_in_bytes"],
        check=False,
    )
    return {
        "anon_mb": v1.get("total_rss", 0) / mb,
        "file_mb": v1.get("total_cache", 0) / mb,
        "current_mb": (float(cur) if cur.strip().isdigit() else 0.0) / mb,
        "cgroup": 1,
    }


def disk_mb(volume: str) -> float:
    out = sh(
        ["docker", "run", "--rm", "-v", f"{volume}:/v", "alpine:3.20", "du", "-sm", "/v"],
        check=False,
    )
    try:
        return float(out.split()[0])
    except (ValueError, IndexError):
        return 0.0


# ── 클라이언트 ──


def chroma_collection():
    import chromadb

    client = chromadb.HttpClient(host="127.0.0.1", port=CHROMA_PORT)
    return client.get_or_create_collection(COLLECTION, metadata={"hnsw:space": "cosine"})


def qdrant_index():
    import os

    os.environ["QDRANT_PORT"] = str(QDRANT_PORT)
    os.environ["QDRANT_HOST"] = "127.0.0.1"
    os.environ["QDRANT_COLLECTION_NAME"] = COLLECTION
    os.environ["VECTOR_BACKEND"] = "qdrant"
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from app.services.vector_index import QdrantIndex

    idx = QdrantIndex()
    idx.ensure_collection(vector_size=DIM)
    return idx


def wait_qdrant_settled(timeout: float = 900.0) -> None:
    """세그먼트 최적화(양자화본 생성·mmap 전환)가 끝날 때까지 대기.

    재시작 직후엔 옵티마이저가 아직 돌고 있어 메모리가 정상상태보다 부풀어 있다.
    이걸 안 기다리고 재면 Qdrant 쪽 수치가 실제보다 나쁘게 나온다.
    """
    import urllib.request

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            raw = urllib.request.urlopen(
                f"http://127.0.0.1:{QDRANT_PORT}/collections/{COLLECTION}", timeout=10
            ).read()
            res = json.loads(raw)["result"]
            if res.get("status") == "green" and res.get("optimizer_status") == "ok":
                # 상태가 green 이어도 직후 몇 초는 페이지 반납이 덜 됐다.
                time.sleep(10)
                return
        except Exception:
            pass
        time.sleep(5)
    print("[bench] 경고: Qdrant 최적화 대기 시간 초과 — 수치가 정상상태보다 높을 수 있음")


def wait_ready(url: str, timeout: float = 120.0) -> None:
    import urllib.request

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=3).read()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"not ready: {url}")


# ── 적재 / 측정 ──


def make_batch(start: int, n: int, rng) -> Dict[str, Any]:
    vecs = rng.random((n, DIM), dtype=np.float32)
    # 실제 임베딩은 정규화돼 있다 — 분포를 맞춰야 양자화 오차 특성이 현실과 비슷해진다.
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
    ids = [f"{start + j:012d}" for j in range(n)]
    metas = [
        {
            "tenant_id": "bench",
            "file_id": f"f{(start + j) % 2000}",
            "chunk_id": f"c{start + j}",
            "type": "document",
        }
        for j in range(n)
    ]
    return {"ids": ids, "vectors": vecs, "metadatas": metas}


def latency_ms(fn, queries, warmup: int = 5) -> Dict[str, float]:
    for q in queries[:warmup]:
        fn(q)
    samples = []
    for q in queries:
        t0 = time.perf_counter()
        fn(q)
        samples.append((time.perf_counter() - t0) * 1000)
    samples.sort()
    return {
        "p50": statistics.median(samples),
        "p95": samples[int(len(samples) * 0.95) - 1],
        "mean": statistics.fmean(samples),
    }


def run(target: int, checkpoints: List[int], batch: int, n_queries: int) -> List[Dict[str, Any]]:
    import uuid as _uuid

    rng = np.random.default_rng(42)

    print(f"[bench] 컨테이너 기동 (dim={DIM}, target={target:,})")
    container_up(
        CHROMA_NAME, CHROMA_IMAGE, CHROMA_PORT, 8000, "bench_chroma_vol",
        ["run", "--path", "/data", "--host", "0.0.0.0", "--port", "8000"],
    )
    container_up(QDRANT_NAME, QDRANT_IMAGE, QDRANT_PORT, 6333, "bench_qdrant_vol", [])
    wait_ready(f"http://127.0.0.1:{CHROMA_PORT}/api/v2/heartbeat")
    wait_ready(f"http://127.0.0.1:{QDRANT_PORT}/")

    col = chroma_collection()
    qidx = qdrant_index()

    query_vecs = rng.random((n_queries, DIM), dtype=np.float32)
    query_vecs /= np.linalg.norm(query_vecs, axis=1, keepdims=True)
    queries = [v.tolist() for v in query_vecs]

    # 빈 컨테이너 기준선 — 벡터 1건당 한계 메모리(기울기)를 구하려면 절편이 필요하다.
    baseline = {
        "count": 0,
        "chroma_mem": mem_stats(CHROMA_NAME),
        "qdrant_mem": mem_stats(QDRANT_NAME),
    }
    print(f"[bench] 기준선(빈 컨테이너): {json.dumps(baseline, ensure_ascii=False)}", flush=True)

    rows: List[Dict[str, Any]] = [baseline]
    inserted = 0
    t_chroma = 0.0
    t_qdrant = 0.0

    for cp in checkpoints:
        while inserted < cp:
            n = min(batch, cp - inserted)
            b = make_batch(inserted, n, rng)
            vecs_list = b["vectors"].tolist()

            t0 = time.perf_counter()
            col.upsert(ids=b["ids"], embeddings=vecs_list, metadatas=b["metadatas"])
            t_chroma += time.perf_counter() - t0

            # Qdrant point id 는 UUID — 벤치 id 를 결정적으로 사상한다.
            qids = [str(_uuid.uuid5(_uuid.NAMESPACE_URL, i)) for i in b["ids"]]
            t0 = time.perf_counter()
            qidx.upsert(
                ids=qids, embeddings=vecs_list, documents=[""] * n,
                metadatas=b["metadatas"], wait=False,
            )
            t_qdrant += time.perf_counter() - t0

            inserted += n
            print(f"[bench]   적재 {inserted:,}/{target:,}", flush=True)

        print(f"[bench] 체크포인트 {cp:,} — 최적화 정착 대기 후 재시작·측정")
        # 적재 직후엔 옵티마이저가 돌고 있다. 먼저 정착시킨 뒤 재시작해야
        # '서빙 정상상태' 메모리를 재는 게 된다.
        wait_qdrant_settled()
        container_restart(CHROMA_NAME)
        container_restart(QDRANT_NAME)
        wait_ready(f"http://127.0.0.1:{CHROMA_PORT}/api/v2/heartbeat")
        wait_ready(f"http://127.0.0.1:{QDRANT_PORT}/")
        wait_qdrant_settled()

        col2 = chroma_collection()
        qidx2 = qdrant_index()

        chroma_lat = latency_ms(
            lambda q: col2.query(query_embeddings=[q], n_results=5, include=[]), queries
        )
        qdrant_lat = latency_ms(lambda q: qidx2.query(embedding=q, top_k=5), queries)

        # 필터 있는 검색 — 실서비스는 항상 tenant_id 로 좁힌다.
        chroma_lat_f = latency_ms(
            lambda q: col2.query(
                query_embeddings=[q], n_results=5, where={"tenant_id": "bench"}, include=[]
            ),
            queries,
        )
        qdrant_lat_f = latency_ms(
            lambda q: qidx2.query(embedding=q, top_k=5, where={"tenant_id": "bench"}), queries
        )

        row = {
            "count": cp,
            "chroma_mem": mem_stats(CHROMA_NAME),
            "qdrant_mem": mem_stats(QDRANT_NAME),
            "chroma_disk_mb": disk_mb("bench_chroma_vol"),
            "qdrant_disk_mb": disk_mb("bench_qdrant_vol"),
            "chroma_lat": chroma_lat,
            "qdrant_lat": qdrant_lat,
            "chroma_lat_filtered": chroma_lat_f,
            "qdrant_lat_filtered": qdrant_lat_f,
            "chroma_insert_sec": t_chroma,
            "qdrant_insert_sec": t_qdrant,
        }
        rows.append(row)
        print(json.dumps(row, ensure_ascii=False, indent=2), flush=True)

    return rows


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--target", type=int, default=200_000)
    p.add_argument("--checkpoints", default="50000,100000,200000")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--queries", type=int, default=30)
    p.add_argument("--out", type=Path, default=Path("bench_result.json"))
    p.add_argument("--keep", action="store_true", help="끝나고 컨테이너/볼륨 유지")
    args = p.parse_args()

    checkpoints = [int(x) for x in args.checkpoints.split(",") if x.strip()]
    try:
        rows = run(args.target, checkpoints, args.batch, args.queries)
        args.out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[bench] 저장: {args.out}")
    finally:
        if not args.keep:
            sh(["docker", "rm", "-f", CHROMA_NAME], check=False)
            sh(["docker", "rm", "-f", QDRANT_NAME], check=False)
            sh(["docker", "volume", "rm", "bench_chroma_vol"], check=False)
            sh(["docker", "volume", "rm", "bench_qdrant_vol"], check=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
