"""QDRANT_ON_DISK 의 실제 비용 측정 — true / false 를 같은 데이터로 통제 비교.

"원본을 디스크에 두면 얼마나 느려지는가"를 따로 떼어 잰다. 두 Qdrant 컨테이너에 완전히
동일한 벡터를 넣고, 컬렉션 설정만 on_disk 로 갈라 지연·메모리를 비교한다.
(양자화는 양쪽 다 int8 유지 — on_disk 하나만 변수로 둔다.)

usage: python benchmark/bench_on_disk_effect.py --count 100000
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DIM = 1536
IMAGE = "qdrant/qdrant:v1.19.0"
VARIANTS = [
    {"name": "bench-ondisk-true", "port": 6353, "vol": "bench_ondisk_true", "on_disk": True},
    {"name": "bench-ondisk-false", "port": 6354, "vol": "bench_ondisk_false", "on_disk": False},
]


def sh(cmd: List[str], check: bool = True) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n{p.stderr}")
    return p.stdout.strip()


def up(v: Dict[str, Any]) -> None:
    sh(["docker", "rm", "-f", v["name"]], check=False)
    sh(["docker", "volume", "rm", v["vol"]], check=False)
    sh(["docker", "volume", "create", v["vol"]])
    sh([
        "docker", "run", "-d", "--name", v["name"], "-p", f"{v['port']}:6333",
        "-v", f"{v['vol']}:/qdrant/storage",
        "-e", "QDRANT__SERVICE__MAX_REQUEST_SIZE_MB=256",
        IMAGE,
    ])


def wait_ready(port: int, timeout: float = 120) -> None:
    import urllib.request

    end = time.time() + timeout
    while time.time() < end:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=3).read()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"qdrant :{port} not ready")


def wait_settled(port: int, timeout: float = 900) -> None:
    import urllib.request

    end = time.time() + timeout
    while time.time() < end:
        try:
            res = json.loads(
                urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/collections/documents", timeout=10
                ).read()
            )["result"]
            if res.get("status") == "green" and res.get("optimizer_status") == "ok":
                time.sleep(10)
                return
        except Exception:
            pass
        time.sleep(5)


def mem_mb(name: str) -> Dict[str, float]:
    out = sh(["docker", "exec", name, "cat", "/sys/fs/cgroup/memory/memory.stat"], check=False)
    st: Dict[str, float] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                st[parts[0]] = float(parts[1])
            except ValueError:
                pass
    mb = 1024 * 1024
    return {"anon_mb": st.get("total_rss", 0) / mb, "file_mb": st.get("total_cache", 0) / mb}


def make_index(v: Dict[str, Any]):
    os.environ["QDRANT_HOST"] = "127.0.0.1"
    os.environ["QDRANT_PORT"] = str(v["port"])
    os.environ["QDRANT_COLLECTION_NAME"] = "documents"
    os.environ["QDRANT_ON_DISK"] = "true" if v["on_disk"] else "false"
    os.environ["QDRANT_QUANTIZATION"] = "int8"
    import importlib

    from app.core import config
    from app.services import vector_index as vi

    importlib.reload(config)
    importlib.reload(vi)
    idx = vi.QdrantIndex()
    idx.ensure_collection(vector_size=DIM)
    return idx


def latency(fn, queries) -> Dict[str, float]:
    for q in queries[:5]:
        fn(q)
    s = []
    for q in queries:
        t0 = time.perf_counter()
        fn(q)
        s.append((time.perf_counter() - t0) * 1000)
    s.sort()
    return {"p50": statistics.median(s), "p95": s[int(len(s) * 0.95) - 1]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=100_000)
    ap.add_argument("--batch", type=int, default=2500)
    ap.add_argument("--queries", type=int, default=50)
    ap.add_argument("--out", type=Path, default=Path("bench_on_disk.json"))
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    rng = np.random.default_rng(7)
    qv = rng.random((args.queries, DIM), dtype=np.float32)
    qv /= np.linalg.norm(qv, axis=1, keepdims=True)
    queries = [v.tolist() for v in qv]

    results: List[Dict[str, Any]] = []
    try:
        for v in VARIANTS:
            print(f"\n===== on_disk={v['on_disk']} =====")
            up(v)
            wait_ready(v["port"])
            idx = make_index(v)

            # 두 변형에 완전히 같은 벡터가 들어가도록 시드를 매번 고정한다.
            gen = np.random.default_rng(1234)
            done = 0
            while done < args.count:
                n = min(args.batch, args.count - done)
                vecs = gen.random((n, DIM), dtype=np.float32)
                vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
                ids = [str(uuid.uuid5(uuid.NAMESPACE_URL, f"b{done + j}")) for j in range(n)]
                metas = [
                    {"tenant_id": "bench", "file_id": f"f{(done + j) % 2000}", "type": "document"}
                    for j in range(n)
                ]
                idx.upsert(ids=ids, embeddings=vecs.tolist(), documents=[""] * n,
                           metadatas=metas, wait=False)
                done += n
                print(f"  적재 {done:,}/{args.count:,}", flush=True)

            wait_settled(v["port"])
            sh(["docker", "restart", v["name"]])
            wait_ready(v["port"])
            wait_settled(v["port"])

            idx2 = make_index(v)
            plain = latency(lambda q: idx2.query(embedding=q, top_k=5), queries)
            filt = latency(
                lambda q: idx2.query(embedding=q, top_k=5, where={"tenant_id": "bench"}), queries
            )
            mem_light = mem_mb(v["name"])

            # 질의 몇십 번으로는 원본 벡터를 거의 안 건드려서 on_disk 차이가 안 드러난다.
            # 전체 원본을 한 번씩 읽어, 오래 운영해 데이터가 두루 조회된 상태를 만든다.
            print("  전수 접근(원본 벡터 모두 읽기)...", flush=True)
            offset = None
            while True:
                points, offset = idx2.client.scroll(
                    collection_name="documents", limit=5000, offset=offset,
                    with_vectors=True, with_payload=False,
                )
                if offset is None:
                    break
            mem_full = mem_mb(v["name"])

            row = {
                "on_disk": v["on_disk"],
                "count": args.count,
                "mem": mem_light,
                "mem_after_full_scan": mem_full,
                "lat_plain": plain,
                "lat_filtered": filt,
            }
            results.append(row)
            print(json.dumps(row, ensure_ascii=False, indent=2), flush=True)

        args.out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

        print("\n" + "=" * 70)
        print(f"QDRANT_ON_DISK 효과 ({args.count:,} 벡터, int8 양자화 동일)")
        print("=" * 70)
        print(f"{'on_disk':>9} | {'anon 초기':>10} {'anon 전수후':>11} {'file 전수후':>11} | "
              f"{'무필터':>8} {'필터':>8}")
        print("-" * 70)
        for r in results:
            m, mf = r["mem"], r["mem_after_full_scan"]
            print(f"{str(r['on_disk']):>9} | {m['anon_mb']:>8,.0f}MB {mf['anon_mb']:>9,.0f}MB "
                  f"{mf['file_mb']:>9,.0f}MB | {r['lat_plain']['p50']:>6.1f}ms "
                  f"{r['lat_filtered']['p50']:>6.1f}ms")
        if len(results) == 2:
            t, f = results[0], results[1]
            print("-" * 70)
            # 전수 접근 후의 anon 차이가 on_disk 의 실제 RAM 효과다.
            saved = f["mem_after_full_scan"]["anon_mb"] - t["mem_after_full_scan"]["anon_mb"]
            cost = t["lat_plain"]["p50"] - f["lat_plain"]["p50"]
            print(f"true 로 절약한 RAM(전수 접근 후) : {saved:>8,.0f} MB")
            print(f"그 대가로 늘어난 무필터 지연     : {cost:>8,.1f} ms")
            print(f"(참고: 원본 벡터 총량 = {args.count * DIM * 4 / 1024 ** 2:,.0f} MB)")
    finally:
        if not args.keep:
            for v in VARIANTS:
                sh(["docker", "rm", "-f", v["name"]], check=False)
                sh(["docker", "volume", "rm", v["vol"]], check=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
