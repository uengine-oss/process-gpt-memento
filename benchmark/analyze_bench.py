"""벤치 결과(bench_result.json) → 보고용 수치 환산.

측정 구간에서 '벡터 1건당 한계 메모리'(기울기)를 최소제곱으로 뽑아 목표 규모로 외삽한다.
절편(빈 컨테이너 기준선)은 규모와 무관한 고정 오버헤드라 기울기와 분리해 다룬다.

usage: python benchmark/analyze_bench.py bench_result.json --target 5000000
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8", errors="replace")

DIM = 1536


def linfit(xs: List[float], ys: List[float]) -> Tuple[float, float]:
    """최소제곱 y = a*x + b. (기울기, 절편)"""
    n = len(xs)
    if n < 2:
        return (0.0, ys[0] if ys else 0.0)
    mx = sum(xs) / n
    my = sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return (0.0, my)
    a = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom
    return (a, my - a * mx)


def series(rows: List[Dict[str, Any]], engine: str, field: str):
    xs, ys = [], []
    for r in rows:
        mem = r.get(f"{engine}_mem")
        if not mem or r["count"] == 0:
            continue
        xs.append(float(r["count"]))
        ys.append(float(mem.get(field, 0.0)))
    return xs, ys


def fmt_gb(mb: float) -> str:
    return f"{mb / 1024:,.2f} GB"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("result", type=Path)
    p.add_argument("--target", type=int, default=5_000_000)
    args = p.parse_args()

    rows = json.loads(args.result.read_text(encoding="utf-8"))
    loaded = [r for r in rows if r["count"] > 0]
    target = args.target

    print("=" * 78)
    print(f"측정 구간 (dim={DIM})")
    print("=" * 78)
    print(f"{'벡터 수':>10} | {'Chroma anon':>12} {'Chroma file':>12} | "
          f"{'Qdrant anon':>12} {'Qdrant file':>12}")
    print("-" * 78)
    for r in rows:
        c, q = r.get("chroma_mem", {}), r.get("qdrant_mem", {})
        print(f"{r['count']:>10,} | {c.get('anon_mb', 0):>10,.0f}MB {c.get('file_mb', 0):>10,.0f}MB | "
              f"{q.get('anon_mb', 0):>10,.0f}MB {q.get('file_mb', 0):>10,.0f}MB")

    print()
    print("=" * 78)
    print("검색 지연 (엔진 + 로컬 네트워크 왕복, 임베딩 호출 제외)")
    print("=" * 78)
    print(f"{'벡터 수':>10} | {'Chroma p50':>11} {'Chroma p95':>11} | "
          f"{'Qdrant p50':>11} {'Qdrant p95':>11}")
    print("-" * 78)
    for r in loaded:
        cl, ql = r["chroma_lat_filtered"], r["qdrant_lat_filtered"]
        print(f"{r['count']:>10,} | {cl['p50']:>9.1f}ms {cl['p95']:>9.1f}ms | "
              f"{ql['p50']:>9.1f}ms {ql['p95']:>9.1f}ms")
    print("(tenant_id 필터 적용 — 실서비스와 동일 조건)")

    print()
    print("=" * 78)
    print(f"실규모 외삽: {target:,} 벡터")
    print("=" * 78)

    out: Dict[str, Any] = {"target": target}
    for engine, label in (("chroma", "Chroma"), ("qdrant", "Qdrant")):
        for field in ("anon_mb", "file_mb"):
            xs, ys = series(rows, engine, field)
            a, b = linfit(xs, ys)
            per_vec = a * 1024 * 1024  # MB/vector → bytes/vector
            projected = a * target + b
            out[f"{engine}_{field}_per_vector_bytes"] = per_vec
            out[f"{engine}_{field}_projected_mb"] = projected
            if field == "anon_mb":
                print(f"{label:>7} anon(회수 불가): {per_vec:>8,.0f} B/vec → {fmt_gb(projected):>12}")
            else:
                print(f"{label:>7} file(회수 가능): {per_vec:>8,.0f} B/vec → {fmt_gb(projected):>12}")

    c_anon = out["chroma_anon_mb_projected_mb"]
    q_anon = out["qdrant_anon_mb_projected_mb"]
    if q_anon > 0:
        print()
        print(f"  ▶ 실질 RAM 점유(anon) 절감: {fmt_gb(c_anon)} → {fmt_gb(q_anon)} "
              f"({c_anon / q_anon:.1f}배 감소, {(1 - q_anon / c_anon) * 100:.0f}% 절감)")

    print()
    print("검색 지연 외삽 (tenant_id 필터 = 실서비스 경로)")
    print("-" * 78)
    import math

    n_lo, n_hi = loaded[0]["count"], loaded[-1]["count"]
    data_growth = n_hi / n_lo

    def fmt_ms(v: float) -> str:
        return f"{v / 1000:,.1f} s" if v >= 1000 else f"{v:,.0f} ms"

    for engine, label in (("chroma", "Chroma"), ("qdrant", "Qdrant")):
        xs = [float(r["count"]) for r in loaded]
        ys = [r[f"{engine}_lat_filtered"]["p50"] for r in loaded]
        lat_growth = ys[-1] / ys[0] if ys[0] else float("inf")

        # 데이터가 N배 늘 때 지연도 N배면 전수 스캔(O(n)). 훨씬 덜 늘면 인덱스가 duty 를 한다.
        linear_like = lat_growth >= 0.7 * data_growth
        if linear_like:
            a, b = linfit(xs, ys)
            proj = fmt_ms(a * target + b)
            shape = f"O(n) 전수 스캔 — 데이터 {data_growth:.0f}배에 지연 {lat_growth:.1f}배"
        else:
            # HNSW 는 대략 O(log n). 측정 구간의 로그 기울기로 외삽한다.
            slope = (ys[-1] - ys[0]) / (math.log(n_hi) - math.log(n_lo))
            proj = fmt_ms(ys[-1] + slope * (math.log(target) - math.log(n_hi)))
            shape = f"sublinear(≈O(log n)) — 데이터 {data_growth:.0f}배에 지연 {lat_growth:.1f}배"
        print(f"  {label:>7} p50 {ys[0]:>6.0f}→{ys[-1]:>6.0f}ms  ⇒ {proj:>9} @ {target:,}")
        print(f"          [{shape}]")
    print("  ※ Qdrant 는 측정점 3개라 외삽이 근사치다. 확실한 것은 '규모에 선형 비례하지")
    print("     않는다'는 점이고, Chroma 의 선형 증가는 3점이 거의 정확히 비례해 확실하다.")

    print()
    print("이론 검산 (설계값이 실측과 맞는지 대조)")
    print("-" * 78)
    raw_gb = target * DIM * 4 / 1024 ** 3
    int8_gb = target * DIM * 1 / 1024 ** 3
    hnsw_gb = target * 32 * 4 / 1024 ** 3  # m=16 → layer0 링크 32개, u32
    print(f"  원본 float32 벡터      : {raw_gb:>7,.2f} GB   (Chroma 가 RAM 에 상주시키는 대상)")
    print(f"  int8 양자화본          : {int8_gb:>7,.2f} GB   (Qdrant 가 RAM 에 상주시키는 대상)")
    print(f"  HNSW 링크(m=16)        : {hnsw_gb:>7,.2f} GB")
    print(f"  이론 절감비            : {raw_gb / int8_gb:>7,.1f}배")

    disk_c = [r for r in loaded if r.get("chroma_disk_mb")]
    if disk_c:
        last = loaded[-1]
        n = last["count"]
        print()
        print("디스크 사용량 (실측 → 외삽)")
        print("-" * 78)
        for engine, label in (("chroma", "Chroma"), ("qdrant", "Qdrant")):
            per = last[f"{engine}_disk_mb"] / n
            print(f"  {label:>7}: {last[f'{engine}_disk_mb']:>8,.0f} MB @ {n:,} → "
                  f"{fmt_gb(per * target):>12} @ {target:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
