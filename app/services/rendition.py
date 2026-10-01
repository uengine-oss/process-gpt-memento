"""rendition — 흐르는 문서(HWPX·DOCX 등)를 보기용 PDF로 그리고 블록을 그 위에 맞춘다.

쪽 번호는 변환본 기준이다. 한글·Word 로 연 쪽과 다를 수 있어 인용 앵커로 쓰지 않는다
(docs/DESIGN_NOTES.md#흐르는-문서의-쪽-번호). 앵커는 여전히 블록이고, 여기서는 블록을
변환본 위에 칠할 위치(쪽·줄 bbox)만 구한다.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
from bisect import bisect_left
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import PROJECT_ROOT

logger = logging.getLogger(__name__)

RENDITION_VERSION = "1"
ALIGN_VERSION = "4"
# 비공개 산출물 버킷(키 접두사로 버킷이 정해진다 — app/storage/artifact_bucket.py).
STORAGE_PREFIX = "artifacts/renditions/"
PREWARM = os.getenv("RENDITION_PREWARM", "true").lower() not in {"0", "false", "no"}
# 대량 인제스트 때 렌더러(rhwp·soffice)가 CPU 를 다 먹지 않게 한 번에 하나씩.
_prewarm_gate = asyncio.Semaphore(1)
_background: set = set()
CACHE_DIR = Path(os.getenv("RENDITION_CACHE_DIR") or PROJECT_ROOT / ".cache" / "renditions")
RHWP_TIMEOUT = 300
# 파서 텍스트와 렌더러 텍스트가 어긋나 전체가 안 맞을 때, 앞·뒤 조각으로라도 자리를 잡는다.
ANCHOR_CHARS = 24
_RHWP_RUNTIME = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/native/rhwp/rhwp.exe"

_locks: Dict[str, asyncio.Lock] = {}


def find_rhwp() -> Optional[str]:
    for cand in (os.getenv("HWPX_RHWP"), shutil.which("rhwp"), str(_RHWP_RUNTIME)):
        if cand and Path(cand).is_file():
            return cand
    return None


def renderer_for(file_name: str) -> Optional[str]:
    ext = Path(file_name).suffix.lower()
    if ext in {".hwpx", ".hwp"}:
        return "rhwp" if find_rhwp() else None
    if ext in {".docx", ".doc", ".rtf", ".odt"}:
        return "soffice"
    return None


def _render_pdf(src: Path, out: Path, renderer: str) -> None:
    if renderer == "rhwp":
        proc = subprocess.run(
            [find_rhwp(), "export-pdf", str(src), "-o", str(out), "--json"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=RHWP_TIMEOUT,
        )
        if proc.returncode != 0 or not out.is_file():
            raise RuntimeError(f"rhwp export-pdf failed: {proc.stderr.decode('utf-8', 'replace')[-800:]}")
        return
    from app.services.file_to_pdf import convert_to_pdf

    produced = Path(convert_to_pdf(str(src), str(out.parent)))
    produced.replace(out)


def _keep(c: str) -> bool:
    # 렌더러마다 괄호·기호 글리프를 텍스트로 뽑는 방식이 달라(「」 누락 등) 글자·숫자만 맞춘다.
    return unicodedata.category(c)[0] in "LN"


def _squash(text: str) -> str:
    return "".join(c for c in (text or "") if _keep(c))


def _pdf_chars(pdf_path: Path) -> Tuple[str, List[Tuple[int, int, Tuple[float, float, float, float]]], List[Tuple[float, float]]]:
    """공백을 뺀 글자 흐름과 글자별 (쪽, 줄 id, bbox), 쪽 크기."""
    import fitz

    chars: List[str] = []
    where: List[Tuple[int, int, Tuple[float, float, float, float]]] = []
    sizes: List[Tuple[float, float]] = []
    line_id = 0
    with fitz.open(pdf_path) as pdf:
        for page_no, page in enumerate(pdf, start=1):
            sizes.append((page.rect.width, page.rect.height))
            for block in page.get_text("rawdict")["blocks"]:
                for line in block.get("lines", []):
                    line_id += 1
                    for span in line["spans"]:
                        for ch in span["chars"]:
                            c = ch["c"]
                            if not _keep(c):
                                continue
                            chars.append(c)
                            where.append((page_no, line_id, tuple(round(v, 1) for v in ch["bbox"])))
    return "".join(chars), where, sizes


def _line_rects(where: Sequence[Tuple[int, int, Tuple[float, ...]]], start: int, end: int) -> List[Dict[str, Any]]:
    """글자 범위 → 쪽별 줄 단위 사각형."""
    rects: Dict[Tuple[int, int], List[float]] = {}
    for page, line, (x0, y0, x1, y1) in where[start:end]:
        r = rects.get((page, line))
        if r is None:
            rects[(page, line)] = [x0, y0, x1, y1]
        else:
            r[0], r[1], r[2], r[3] = min(r[0], x0), min(r[1], y0), max(r[2], x1), max(r[3], y1)
    # 글꼴이 바뀌는 곳마다 텍스트 줄이 끊겨 조각나므로, 같은 쪽에서 세로로 겹치는 조각을 한 줄로 합친다.
    rows: List[Dict[str, Any]] = []
    for (page, _), r in sorted(rects.items(), key=lambda kv: kv[0][1]):
        last = rows[-1] if rows else None
        if last and last["page"] == page:
            a = last["bbox"]
            overlap = min(a[3], r[3]) - max(a[1], r[1])
            if overlap > 0.5 * min(a[3] - a[1], r[3] - r[1]):
                last["bbox"] = [min(a[0], r[0]), min(a[1], r[1]), max(a[2], r[2]), max(a[3], r[3])]
                continue
        rows.append({"page": page, "bbox": list(r)})
    return rows


def align_blocks(blocks: Sequence[Dict[str, Any]], stream: str, where: Sequence[Tuple[int, int, Tuple[float, ...]]]) -> Dict[int, Dict[str, Any]]:
    """블록을 변환본 글자 흐름에 맞춘다.

    1) 변환본에 한 번만 나오는 블록 중 문서 순서와 어긋나지 않는 것(최장 증가 부분열)을 고정점으로.
    2) 나머지는 앞뒤 고정점 사이에서만 순서대로 찾는다. 같은 문장이 여러 번 나와도 제자리에 붙는다.
    """
    texts = [(b["block_index"], _squash(b.get("text") or "")) for b in blocks]
    texts = [(i, t) for i, t in texts if t]

    unique: List[Tuple[int, int]] = []  # (texts 순번, 변환본 위치)
    for order, (_, t) in enumerate(texts):
        first = stream.find(t)
        if first >= 0 and stream.find(t, first + 1) < 0:
            unique.append((order, first))
    anchors = _increasing(unique)

    spans: Dict[int, Tuple[int, int, str]] = {}
    for order, pos in anchors:
        spans[order] = (pos, pos + len(texts[order][1]), "anchor")
    bounds = [(-1, 0)] + [(o, spans[o][1]) for o, _ in anchors] + [(len(texts), len(stream))]
    for (lo_order, lo_pos), (hi_order, _) in zip(bounds, bounds[1:]):
        hi_pos = spans[hi_order][0] if hi_order in spans else len(stream)
        cursor = lo_pos
        for order in range(lo_order + 1, hi_order):
            span = _find_in(stream, texts[order][1], cursor, hi_pos)
            if span:
                spans[order] = span
                cursor = span[1]

    placed: Dict[int, Dict[str, Any]] = {}
    for order, (start, end, how) in spans.items():
        placed[texts[order][0]] = {"rects": _line_rects(where, start, end), "match": how}
    return placed


def _increasing(pairs: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """(순번, 위치) 중 위치가 순번과 같이 커지는 가장 긴 부분열."""
    tails: List[int] = []
    tail_idx: List[int] = []
    prev = [-1] * len(pairs)
    for i, (_, pos) in enumerate(pairs):
        k = bisect_left(tails, pos)
        if k == len(tails):
            tails.append(pos)
            tail_idx.append(i)
        else:
            tails[k] = pos
            tail_idx[k] = i
        prev[i] = tail_idx[k - 1] if k else -1
    out: List[Tuple[int, int]] = []
    i = tail_idx[-1] if tail_idx else -1
    while i >= 0:
        out.append(pairs[i])
        i = prev[i]
    return out[::-1]


def _find_in(stream: str, text: str, lo: int, hi: int) -> Optional[Tuple[int, int, str]]:
    pos = stream.find(text, lo, hi)
    if pos >= 0:
        return pos, pos + len(text), "exact"
    if len(text) <= ANCHOR_CHARS:
        return None
    head = stream.find(text[:ANCHOR_CHARS], lo, hi)
    tail = stream.find(text[-ANCHOR_CHARS:], head if head >= 0 else lo, hi)
    if head >= 0 and tail >= 0 and tail - head < len(text) * 2:
        return head, tail + ANCHOR_CHARS, "anchored"
    if head >= 0:
        return head, min(hi, head + len(text)), "head"
    if tail >= 0:
        return max(lo, tail + ANCHOR_CHARS - len(text)), tail + ANCHOR_CHARS, "tail"
    return None


def _cache_keys(file_hash: str, renderer: str, blocks: Sequence[Dict[str, Any]]) -> Tuple[str, str]:
    """변환본 키(원본·렌더러)와 배치 키(+정렬 방식·블록 내용). 재인덱싱으로 블록이 바뀌면 배치만 다시 한다."""
    pdf_key = hashlib.sha256(f"{file_hash}:{renderer}:{RENDITION_VERSION}".encode()).hexdigest()[:24]
    digest = hashlib.sha256("\x1e".join(b.get("text") or "" for b in blocks).encode()).hexdigest()[:12]
    return pdf_key, f"{pdf_key}.align{ALIGN_VERSION}.{digest}"


async def _storage_get(key: str) -> Optional[bytes]:
    from app.core.supabase_client import supabase
    from app.storage.artifact_bucket import bucket_for

    try:
        return await asyncio.to_thread(supabase.storage.from_(bucket_for(key)).download, key)
    except Exception:  # noqa: BLE001 - 없으면 새로 만든다
        return None


async def _storage_has(key: str) -> bool:
    from app.core.supabase_client import supabase
    from app.storage.artifact_bucket import bucket_for

    folder, _, name = key.rpartition("/")
    try:
        found = await asyncio.to_thread(supabase.storage.from_(bucket_for(key)).list, folder, {"search": name})
    except Exception:  # noqa: BLE001
        return False
    return any(item.get("name") == name for item in found or [])


async def _storage_put(key: str, data: bytes, content_type: str) -> None:
    from app.core.supabase_client import supabase
    from app.storage.artifact_bucket import bucket_for

    try:
        await asyncio.to_thread(
            supabase.storage.from_(bucket_for(key)).upload, key, data,
            {"content-type": content_type, "upsert": "true"},
        )
    except Exception as exc:  # noqa: BLE001 - 로컬 캐시로는 계속 동작한다
        logger.warning("[rendition] storage upload failed (%s): %s", key, exc)


async def ensure_rendition(
    *, file_id: str, file_name: str, file_hash: str, blocks: Sequence[Dict[str, Any]], load_bytes,
) -> Optional[Dict[str, Any]]:
    """변환본 PDF 경로와 블록 배치. 렌더러가 없는 형식이면 None.

    로컬 캐시 → 스토리지(STORAGE_PREFIX) → 새로 변환 순. 새로 만든 것은 둘 다에 남긴다(파드가 바뀌어도 재사용).
    """
    renderer = renderer_for(file_name)
    if not renderer:
        return None
    pdf_key, map_key = _cache_keys(file_hash or file_id, renderer, blocks)
    pdf_path, map_path = CACHE_DIR / f"{pdf_key}.pdf", CACHE_DIR / f"{map_key}.json"
    lock = _locks.setdefault(pdf_key, asyncio.Lock())
    async with lock:
        if pdf_path.is_file() and map_path.is_file():
            return {**json.loads(map_path.read_text(encoding="utf-8")), "pdf_path": str(pdf_path)}
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        if not pdf_path.is_file():
            stored = await _storage_get(f"{STORAGE_PREFIX}{pdf_key}.pdf")
            if stored:
                pdf_path.write_bytes(stored)
            else:
                raw = await load_bytes()
                with tempfile.TemporaryDirectory() as tmp:
                    src = Path(tmp) / f"source{Path(file_name).suffix.lower()}"
                    src.write_bytes(raw)
                    out = Path(tmp) / "rendition.pdf"
                    await asyncio.to_thread(_render_pdf, src, out, renderer)
                    shutil.move(str(out), pdf_path)
                await _storage_put(f"{STORAGE_PREFIX}{pdf_key}.pdf", pdf_path.read_bytes(), "application/pdf")
        elif not await _storage_has(f"{STORAGE_PREFIX}{pdf_key}.pdf"):
            await _storage_put(f"{STORAGE_PREFIX}{pdf_key}.pdf", pdf_path.read_bytes(), "application/pdf")
        stored_map = await _storage_get(f"{STORAGE_PREFIX}{map_key}.json")
        if stored_map:
            map_path.write_bytes(stored_map)
            return {**json.loads(stored_map.decode("utf-8")), "pdf_path": str(pdf_path)}
        stream, where, sizes = await asyncio.to_thread(_pdf_chars, pdf_path)
        placed = align_blocks(blocks, stream, where)
        data = {
            "renderer": renderer,
            "page_count": len(sizes),
            "page_sizes": sizes,
            "placed": {str(k): v for k, v in placed.items()},
            "coverage": {"blocks": sum(1 for b in blocks if _squash(b.get("text") or "")), "placed": len(placed)},
        }
        encoded = json.dumps(data, ensure_ascii=False).encode("utf-8")
        map_path.write_bytes(encoded)
        await _storage_put(f"{STORAGE_PREFIX}{map_key}.json", encoded, "application/json")
        logger.info("[rendition] %s via %s: %d쪽, 블록 %d/%d 배치",
                    file_name, renderer, len(sizes), len(placed), data["coverage"]["blocks"])
        return {**data, "pdf_path": str(pdf_path)}


async def prewarm(tenant_id: str, file_id: str) -> None:
    """인제스트 직후 변환본을 미리 만든다. 첫 열람이 변환을 기다리지 않게. 실패는 열람 때 다시 시도된다."""
    from app.core.supabase_client import supabase
    from app.storage.artifact_bucket import bucket_for

    async with _prewarm_gate:
        try:
            rows = (await asyncio.to_thread(
                supabase.table("knowledge_files").select("file_name, file_hash")
                .eq("tenant_id", tenant_id).eq("source_ref", file_id).limit(1).execute
            )).data or []
            if not rows or not renderer_for(rows[0]["file_name"] or ""):
                return
            from app.api.citations import _all_blocks

            blocks = await _all_blocks(tenant_id, file_id)
            await ensure_rendition(
                file_id=file_id, file_name=rows[0]["file_name"], file_hash=rows[0].get("file_hash") or "",
                blocks=blocks,
                load_bytes=lambda: asyncio.to_thread(supabase.storage.from_(bucket_for(file_id)).download, file_id),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[rendition] prewarm failed (%s/%s): %s", tenant_id, file_id, exc)


def schedule_prewarm(tenant_id: str, file_id: str, file_name: str) -> None:
    if PREWARM and renderer_for(file_name or file_id):
        task = asyncio.get_running_loop().create_task(prewarm(tenant_id, file_id))
        _background.add(task)
        task.add_done_callback(_background.discard)
