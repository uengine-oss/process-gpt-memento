"""두 벡터 백엔드의 memento API 응답을 비교한다.

이관이 "개수가 맞다" 수준을 넘어 *서비스 동작이 같은지*까지 확인하려는 스크립트.
같은 질의를 Chroma 백엔드로 띄운 memento 와 Qdrant 백엔드로 띄운 memento 에 각각 쏘고,
반환된 청크의 신원(파일명 + chunk_index)을 순서까지 비교한다.

사용:
    # 1) Chroma 백엔드로 memento 를 띄운 뒤
    python scripts/compare_backend_api.py capture --label chroma --out /tmp/chroma.json
    # 2) Qdrant 백엔드로 다시 띄운 뒤
    python scripts/compare_backend_api.py capture --label qdrant --out /tmp/qdrant.json
    # 3) 비교
    python scripts/compare_backend_api.py diff /tmp/chroma.json /tmp/qdrant.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")


# 임베딩 검색이 실제로 갈리는지 보려면 질의가 서로 다른 주제를 짚어야 한다.
DEFAULT_QUERIES: List[str] = [
    "RFI 제안 요청 절차",
    "비밀유지 계약의 손해배상 조항",
    "재무제표 총자산과 부채 규모",
    "타당성 조사 범위",
    "계약 해지 사유",
]


def _get(base_url: str, path: str, params: Dict[str, Any], timeout: float = 120.0) -> Any:
    query = urllib.parse.urlencode(params, doseq=True)
    url = f"{base_url.rstrip('/')}{path}?{query}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fingerprint(docs: List[Dict[str, Any]]) -> List[str]:
    """청크의 신원 — 백엔드가 달라도 같은 청크면 같은 문자열이어야 한다.

    file_name 은 고유하지 않다(같은 문서를 다른 file_id 로 두 번 올린 경우가 흔하다).
    chunk_id 를 우선 쓰고, 없을 때만 file_id+chunk_index 로 떨어진다.
    """
    out = []
    for d in docs:
        meta = d.get("metadata") or {}
        chunk_id = meta.get("chunk_id")
        if chunk_id:
            out.append(str(chunk_id))
        else:
            out.append(f"{meta.get('file_id')}#{meta.get('chunk_index')}")
    return out


def capture(base_url: str, tenant_id: str, queries: List[str], top_k: int, label: str) -> Dict[str, Any]:
    results: Dict[str, Any] = {"label": label, "tenant_id": tenant_id, "top_k": top_k, "queries": {}}
    for q in queries:
        t0 = time.perf_counter()
        payload = _get(base_url, "/search", {"query": q, "tenant_id": tenant_id, "top_k": top_k})
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        docs = payload.get("response") or []
        results["queries"][q] = {
            "count": len(docs),
            "fingerprints": _fingerprint(docs),
            "elapsed_ms": elapsed_ms,
        }
        print(f"[{label}] {q!r} → {len(docs)} chunks, {elapsed_ms}ms")
    return results


def diff(left_path: Path, right_path: Path) -> bool:
    left = json.loads(left_path.read_text(encoding="utf-8"))
    right = json.loads(right_path.read_text(encoding="utf-8"))
    ll, rl = left["label"], right["label"]

    all_ok = True
    print(f"\n{'질의':<36} {ll:>10} {rl:>10}  {'동일':>6}  {'겹침':>6}")
    print("-" * 78)
    for q, lres in left["queries"].items():
        rres = right["queries"].get(q)
        if rres is None:
            print(f"{q[:34]:<36} {'—':>10} {'없음':>10}")
            all_ok = False
            continue

        lfp, rfp = lres["fingerprints"], rres["fingerprints"]
        identical = lfp == rfp
        overlap = len(set(lfp) & set(rfp))
        denom = max(len(lfp), 1)
        # 순서까지 같으면 이상적이지만, int8 양자화는 근소한 점수차의 순위를 뒤집을 수 있다.
        # 실질 판정 기준은 "같은 청크 집합을 찾았는가"(겹침률).
        if not identical:
            all_ok = False
        print(
            f"{q[:34]:<36} {lres['elapsed_ms']:>8}ms {rres['elapsed_ms']:>8}ms "
            f"{'예' if identical else '아니오':>6}  {overlap}/{denom:>3}"
        )

        if not identical:
            only_l = [x for x in lfp if x not in rfp]
            only_r = [x for x in rfp if x not in lfp]
            if only_l:
                print(f"    {ll}에만: {only_l}")
            if only_r:
                print(f"    {rl}에만: {only_r}")
            if not only_l and not only_r:
                print(f"    (집합은 동일, 순서만 다름)\n      {ll}: {lfp}\n      {rl}: {rfp}")

    print("-" * 78)
    print("결과: " + ("✓ 완전 일치" if all_ok else "△ 차이 있음 — 위 상세 확인"))
    return all_ok


def main() -> int:
    parser = argparse.ArgumentParser(description="memento 벡터 백엔드 API 응답 비교")
    sub = parser.add_subparsers(dest="cmd", required=True)

    cap = sub.add_parser("capture", help="현재 떠 있는 memento 에 질의해 결과 저장")
    cap.add_argument("--base-url", default="http://127.0.0.1:8005")
    cap.add_argument("--tenant-id", default="localhost")
    cap.add_argument("--top-k", type=int, default=5)
    cap.add_argument("--label", required=True)
    cap.add_argument("--out", type=Path, required=True)
    cap.add_argument("--query", action="append", dest="queries")

    dif = sub.add_parser("diff", help="저장된 두 결과 비교")
    dif.add_argument("left", type=Path)
    dif.add_argument("right", type=Path)

    args = parser.parse_args()

    if args.cmd == "capture":
        results = capture(
            args.base_url, args.tenant_id, args.queries or DEFAULT_QUERIES, args.top_k, args.label
        )
        args.out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[{args.label}] 저장: {args.out}")
        return 0

    return 0 if diff(args.left, args.right) else 1


if __name__ == "__main__":
    raise SystemExit(main())
