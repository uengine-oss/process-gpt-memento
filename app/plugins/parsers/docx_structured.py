"""DOCX 구조화 파서 v2 — 메모·표·placeholder·*이미지* 위치 보존 + Vision 설명.

확장:
- 본문 안 ``<w:drawing>``/``<w:pict>`` 이미지 검출 + 추출
- paragraph 단위 위치 매핑 (어느 paragraph 에 어떤 이미지가)
- 옵션: 멀티모달 LLM (Qwen3.6 vision via OpenAI 호환 API) 으로 *이미지 설명* 받기
- 설명을 blocks/flat_text 의 *해당 위치* 에 inline 박음

출력 구조 (JSON):
{
  "blocks": [
    {
      "type": "paragraph",
      "index": N,
      "text": "...",
      "comments": [...],
      "placeholders": [...],
      "images": [             # NEW
        {
          "rid": "rId7",
          "media_path": "word/media/image1.png",
          "extracted_path": "result/<stem>.images/image1.png",
          "size_bytes": 65369,
          "description": "..."   # --describe 켰을 때 vision LLM 결과
        }
      ]
    },
    {"type": "table", ...}
  ],
  "comments": {...},
  "flat_text": "...",
  "stats": {...}
}

CLI:
  python parse_docx_structured.py <input.docx>
      → text 모드 (flat_text + comments 인덱스 print)
  python parse_docx_structured.py <input.docx> --json
      → JSON 모드
  python parse_docx_structured.py <input.docx> --describe
      → 이미지 검출 시 vision LLM 호출 (env vars CUSTOM_LLM_* 사용)
  python parse_docx_structured.py <input.docx> --out-dir <DIR>
      → 이미지 추출 폴더 지정 (기본: <input docx 와 같은 폴더>/<stem>.images/)
"""
from __future__ import annotations

import base64
import json
import os
import re
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from lxml import etree


W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
A = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
WP = "{http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing}"
PIC = "{http://schemas.openxmlformats.org/drawingml/2006/picture}"
V_NS = "{urn:schemas-microsoft-com:vml}"


# ─────────────────────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────────────────────

def _ln(el) -> str:
    tag = el.tag
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _all_text(el) -> str:
    if el is None:
        return ""
    return "".join((t.text or "") for t in el.iter(W + "t"))


def _paragraph_text(p) -> str:
    parts = []
    for child in p.iter():
        ln = _ln(child)
        if ln == "t":
            parts.append(child.text or "")
        elif ln == "tab":
            parts.append("\t")
        elif ln == "br":
            parts.append("\n")
    return "".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# 메모
# ─────────────────────────────────────────────────────────────────────────────

def parse_comments_xml(data: bytes) -> Dict[str, Dict[str, str]]:
    root = etree.fromstring(data)
    out = {}
    for c in root.iter(W + "comment"):
        cid = c.get(W + "id")
        out[cid] = {
            "author": c.get(W + "author") or "",
            "date": c.get(W + "date") or "",
            "initials": c.get(W + "initials") or "",
            "text": _all_text(c).strip(),
        }
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Placeholder
# ─────────────────────────────────────────────────────────────────────────────

_PLACEHOLDER_RE = re.compile(r"\[[^\[\]\n]+\]")


def find_placeholders_in_text(text: str) -> List[Dict[str, Any]]:
    return [{"marker": m.group(0), "offset": m.start()} for m in _PLACEHOLDER_RE.finditer(text)]


# ─────────────────────────────────────────────────────────────────────────────
# 이미지 검출 (DrawingML 신형 + VML 구형)
# ─────────────────────────────────────────────────────────────────────────────

def _rels_map(rels_xml: bytes) -> Dict[str, str]:
    """rId → Target 경로 (word/ prefix 추가). word/_rels/document.xml.rels."""
    root = etree.fromstring(rels_xml)
    PR = "{http://schemas.openxmlformats.org/package/2006/relationships}"
    out = {}
    for rel in root.iter(PR + "Relationship"):
        rid = rel.get("Id")
        target = rel.get("Target")
        if rid and target:
            # rels 의 Target 은 word/ 기준 상대경로 (예: media/image1.png)
            full = "word/" + target if not target.startswith("/") else target.lstrip("/")
            out[rid] = full
    return out


def _images_in_paragraph(p, rels: Dict[str, str]) -> List[Dict[str, Any]]:
    """paragraph 안의 이미지 rId 들 추출. drawing 신형 + VML 구형 둘 다."""
    out = []
    # 신형: <w:drawing><wp:inline><a:graphic><pic:pic><pic:blipFill><a:blip r:embed="rIdN">
    for blip in p.iter(A + "blip"):
        rid = blip.get(R + "embed") or blip.get(R + "link")
        if rid:
            out.append({"rid": rid, "media_path": rels.get(rid, "")})
    # 구형 VML: <w:pict><v:shape><v:imagedata r:id="rIdN">
    for imgdata in p.iter(V_NS + "imagedata"):
        rid = imgdata.get(R + "id") or imgdata.get(R + "link")
        if rid:
            out.append({"rid": rid, "media_path": rels.get(rid, "")})
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 메모 위치 추적
# ─────────────────────────────────────────────────────────────────────────────

def _extract_comments_in_paragraph(p) -> List[Dict[str, Any]]:
    char_offset = 0
    active_starts: Dict[str, int] = {}
    completed: List[Dict[str, Any]] = []
    reference_only: List[Dict[str, Any]] = []
    for el in p.iter():
        ln = _ln(el)
        if ln == "t":
            char_offset += len(el.text or "")
        elif ln == "tab":
            char_offset += 1
        elif ln == "br":
            char_offset += 1
        elif ln == "commentRangeStart":
            active_starts[el.get(W + "id")] = char_offset
        elif ln == "commentRangeEnd":
            cid = el.get(W + "id")
            if cid in active_starts:
                completed.append({"id": cid, "anchor_span": [active_starts.pop(cid), char_offset]})
        elif ln == "commentReference":
            cid = el.get(W + "id")
            if cid not in active_starts and not any(c["id"] == cid for c in completed):
                reference_only.append({"id": cid, "anchor_span": [char_offset, char_offset]})
    para_text = _paragraph_text(p)
    out = []
    for c in completed + reference_only:
        s, e = c["anchor_span"]
        out.append({
            "id": c["id"],
            "anchor_text": para_text[s:e] if e > s else "",
            "anchor_span": c["anchor_span"],
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 표 → 마크다운
# ─────────────────────────────────────────────────────────────────────────────

def _table_to_markdown(table_el, rels: Dict[str, str]) -> Tuple[str, List[List[Dict[str, Any]]]]:
    rows_data: List[List[Dict[str, Any]]] = []
    for tr in table_el.findall(W + "tr"):
        row = []
        for tc in tr.findall(W + "tc"):
            cell_text_parts = []
            cell_comments: List[Dict[str, Any]] = []
            cell_images: List[Dict[str, Any]] = []
            for p in tc.findall(W + "p"):
                cell_text_parts.append(_paragraph_text(p))
                cell_comments.extend(_extract_comments_in_paragraph(p))
                cell_images.extend(_images_in_paragraph(p, rels))
            cell_text = "\n".join(cell_text_parts).strip()
            row.append({"text": cell_text, "comments": cell_comments, "images": cell_images})
        rows_data.append(row)
    if not rows_data:
        return "(empty table)", rows_data
    num_cols = max(len(r) for r in rows_data)
    md_lines = []
    for ri, row in enumerate(rows_data):
        cells_md = []
        for ci in range(num_cols):
            if ci < len(row):
                cell_txt = row[ci]["text"].replace("\n", " / ").replace("|", "\\|")
                cells_md.append(cell_txt or " ")
            else:
                cells_md.append(" ")
        md_lines.append("| " + " | ".join(cells_md) + " |")
        if ri == 0:
            md_lines.append("| " + " | ".join(["---"] * num_cols) + " |")
    return "\n".join(md_lines), rows_data


# ─────────────────────────────────────────────────────────────────────────────
# 본문 블록 단위
# ─────────────────────────────────────────────────────────────────────────────

def parse_blocks(document_xml: bytes, rels: Dict[str, str]) -> List[Dict[str, Any]]:
    root = etree.fromstring(document_xml)
    body = root.find(W + "body")
    blocks: List[Dict[str, Any]] = []
    para_idx = 0
    table_idx = 0
    for child in body:
        ln = _ln(child)
        if ln == "p":
            text = _paragraph_text(child)
            comments = _extract_comments_in_paragraph(child)
            placeholders = find_placeholders_in_text(text)
            images = _images_in_paragraph(child, rels)
            blocks.append({
                "type": "paragraph",
                "index": para_idx,
                "text": text,
                "comments": comments,
                "placeholders": placeholders,
                "images": images,
            })
            para_idx += 1
        elif ln == "tbl":
            markdown, rows_data = _table_to_markdown(child, rels)
            blocks.append({
                "type": "table",
                "index": table_idx,
                "rows": rows_data,
                "markdown": markdown,
            })
            table_idx += 1
    return blocks


# ─────────────────────────────────────────────────────────────────────────────
# 이미지 추출 + (옵션) Vision LLM 설명
# ─────────────────────────────────────────────────────────────────────────────

VISION_PROMPT = (
    "이 이미지는 MOU 계약 작성용 사업개요서에 포함된 *사업구도/조직도/도식* 입니다. "
    "다음을 한국어로 정리:\n"
    "1. 등장하는 *모든 법인·기관* (예: 한전, AEW, 사우디 아람코, Project Company 등) — 각각 *역할*\n"
    "2. 법인 간 *관계·계약 흐름* (주주간계약 / EPC / O&M / 오프테이크 / 금융 등 — 화살표 방향 포함)\n"
    "3. *지분율* 등 숫자가 있으면 그대로 인용\n\n"
    "출력 형식:\n"
    "- 마크다운 표 (필요 시): 법인 | 역할 | 지분율\n"
    "- 그 뒤에 *계약 관계 요약* 2-3줄\n"
    "원본 도식의 정확한 정보만 사용. 추측 금지."
)


def describe_image_with_vision_llm(image_bytes: bytes, mime_type: str = "image/png") -> str:
    """OpenAI 호환 API 의 vision endpoint 호출.

    env vars:
      CUSTOM_LLM_BASE_URL, CUSTOM_LLM_API_KEY, CUSTOM_LLM_MODEL

    사용자가 deepagents-lite 의 frentis-ai-model 이 멀티모달이라 명시.
    실패 시 빈 문자열 + stderr 로그.
    """
    base_url = os.getenv("CUSTOM_LLM_BASE_URL", "").rstrip("/")
    api_key = os.getenv("CUSTOM_LLM_API_KEY", "not-needed")
    model = os.getenv("CUSTOM_LLM_MODEL", "")
    if not base_url or not model:
        print("[vision] env vars (CUSTOM_LLM_BASE_URL/MODEL) 미설정 — skip", file=sys.stderr)
        return ""
    # base64
    b64 = base64.b64encode(image_bytes).decode("ascii")
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": VISION_PROMPT},
                    {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{b64}"}},
                ],
            },
        ],
        "temperature": 0.2,
        "max_tokens": 4096,
        # Qwen3 reasoning 비활성 — content 채우게 (deepagents-lite 의 _build_docx_model 과 동일)
        "chat_template_kwargs": {"enable_thinking": False},
    }
    try:
        import urllib.request
        req = urllib.request.Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        )
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        choices = data.get("choices") or []
        if not choices:
            return ""
        msg = choices[0].get("message") or {}
        content = msg.get("content")
        # content 비어있으면 reasoning_content fallback (Qwen3 thinking 모드 미차단 시)
        if not content:
            content = msg.get("reasoning_content") or ""
        if isinstance(content, list):
            content = "".join(p.get("text","") if isinstance(p, dict) else str(p) for p in content)
        return str(content).strip()
    except Exception as exc:
        print(f"[vision] 호출 실패: {exc}", file=sys.stderr)
        return ""


def _iter_image_entries(blocks: List[Dict[str, Any]]):
    """모든 image entry 를 등장 순서로 iterate (paragraph + table cell)."""
    for b in blocks:
        if b["type"] == "paragraph":
            for img in b.get("images") or []:
                yield img
        elif b["type"] == "table":
            for row in b.get("rows") or []:
                for cell in row:
                    for img in cell.get("images") or []:
                        yield img


def extract_images_and_describe(
    z: zipfile.ZipFile,
    blocks: List[Dict[str, Any]],
    out_dir: Path,
    describe: bool,
) -> None:
    """blocks 안 이미지 entry 추출 + (옵션) 설명. **vision 호출은 ThreadPoolExecutor 병렬**.

    in-place: 모든 image entry 에 ``extracted_path``, ``size_bytes``, ``description`` 추가.

    Env vars:
        DOCX_VISION_MAX_WORKERS — 동시 vision 호출 수 (기본 4). LLM 서버 부담 제한.
        DOCX_VISION_TIMEOUT_SEC — 각 호출 timeout (기본 180s, urllib 의존). 미사용.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1단계 — 모든 image entry 추출 (binary 저장, dedup, in-place 매핑)
    seen_paths: Dict[str, Path] = {}      # media_path → 저장된 file
    seen_bytes: Dict[str, bytes] = {}     # media_path → raw bytes (vision 호출용)
    entries_by_media: Dict[str, List[Dict[str, Any]]] = {}  # media_path → entry 들

    for img in _iter_image_entries(blocks):
        media_path = img.get("media_path") or ""
        if not media_path:
            continue
        entries_by_media.setdefault(media_path, []).append(img)
        if media_path in seen_paths:
            continue
        try:
            data = z.read(media_path)
        except KeyError:
            print(f"[image] {media_path!r} 추출 실패", file=sys.stderr)
            continue
        fname = Path(media_path).name
        target = out_dir / fname
        target.write_bytes(data)
        seen_paths[media_path] = target
        seen_bytes[media_path] = data

    # 모든 entry 에 extracted_path / size_bytes 채움 (dedup 후 공유)
    for media_path, entries in entries_by_media.items():
        if media_path not in seen_paths:
            continue
        target = seen_paths[media_path]
        sz = target.stat().st_size
        for e in entries:
            e["extracted_path"] = str(target)
            e["size_bytes"] = sz

    if not describe or not seen_bytes:
        return

    # 2단계 — vision LLM 호출 *병렬*. media_path 별 1회만 (dedup).
    import os as _os
    from concurrent.futures import ThreadPoolExecutor, as_completed
    max_workers = max(1, int(_os.getenv("DOCX_VISION_MAX_WORKERS", "4")))

    def _describe_one(media_path: str, data: bytes) -> tuple[str, str]:
        fname = Path(media_path).name
        ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
        mime = {"png":"image/png","jpg":"image/jpeg","jpeg":"image/jpeg",
                "gif":"image/gif","bmp":"image/bmp"}.get(ext, "application/octet-stream")
        print(f"[vision] {fname} → 멀티모달 호출 시작", file=sys.stderr)
        try:
            desc = describe_image_with_vision_llm(data, mime_type=mime)
        except Exception as exc:
            print(f"[vision] {fname} 실패: {exc}", file=sys.stderr)
            desc = ""
        print(f"[vision] {fname} → 완료 ({len(desc)}ch)", file=sys.stderr)
        return media_path, desc

    print(
        f"[vision] {len(seen_bytes)}개 이미지 병렬 처리 시작 (max_workers={max_workers})",
        file=sys.stderr,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_describe_one, mp, data)
            for mp, data in seen_bytes.items()
        ]
        for fut in as_completed(futures):
            try:
                media_path, desc = fut.result()
            except Exception as exc:
                print(f"[vision] future 실패: {exc}", file=sys.stderr)
                continue
            # 같은 media_path 의 모든 entry 에 description 복사
            for e in entries_by_media.get(media_path) or []:
                e["description"] = desc


# ─────────────────────────────────────────────────────────────────────────────
# Flat text view
# ─────────────────────────────────────────────────────────────────────────────

def build_flat_text(blocks: List[Dict[str, Any]], comments_meta: Dict[str, Dict[str, str]]) -> str:
    lines: List[str] = []
    for b in blocks:
        if b["type"] == "paragraph":
            text = b["text"]
            if text.strip():
                lines.append(text)
            for c in b.get("comments") or []:
                meta = comments_meta.get(c["id"]) or {}
                body_txt = (meta.get("text") or "").strip()
                anchor = c.get("anchor_text") or ""
                lines.append(f'  [메모 #{c["id"]} @ "{anchor[:60]}": {body_txt}]')
            for ph in b.get("placeholders") or []:
                lines.append(f'  [placeholder {ph["marker"]} @ offset {ph["offset"]}]')
            for img in b.get("images") or []:
                desc = img.get("description") or "(설명 없음)"
                fname = Path(img.get("extracted_path") or img.get("media_path") or "?").name
                lines.append("")
                lines.append(f'  [IMAGE {fname} @ paragraph {b["index"]}]')
                lines.append(f'  {desc}')
                lines.append(f'  [/IMAGE]')
                lines.append("")
        elif b["type"] == "table":
            lines.append("")
            lines.append(f'[TABLE {b["index"]}]')
            lines.append(b["markdown"])
            for ri, row in enumerate(b.get("rows") or []):
                for ci, cell in enumerate(row):
                    for c in cell.get("comments") or []:
                        meta = comments_meta.get(c["id"]) or {}
                        body_txt = (meta.get("text") or "").strip()
                        anchor = c.get("anchor_text") or ""
                        lines.append(f'  [메모 #{c["id"]} @ TABLE{b["index"]}({ri},{ci}) "{anchor[:60]}": {body_txt}]')
                    for img in cell.get("images") or []:
                        desc = img.get("description") or "(설명 없음)"
                        fname = Path(img.get("extracted_path") or img.get("media_path") or "?").name
                        lines.append(f'  [IMAGE {fname} @ TABLE{b["index"]}({ri},{ci}): {desc[:200]}]')
            lines.append(f'[/TABLE {b["index"]}]')
            lines.append("")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 메인 파서
# ─────────────────────────────────────────────────────────────────────────────

def parse(path: str, out_dir: Optional[Path] = None, describe: bool = False) -> Dict[str, Any]:
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        if "word/document.xml" not in names:
            raise RuntimeError("word/document.xml 없음")
        document_xml = z.read("word/document.xml")
        rels: Dict[str, str] = {}
        if "word/_rels/document.xml.rels" in names:
            rels = _rels_map(z.read("word/_rels/document.xml.rels"))
        comments_meta: Dict[str, Dict[str, str]] = {}
        if "word/comments.xml" in names:
            comments_meta = parse_comments_xml(z.read("word/comments.xml"))

        blocks = parse_blocks(document_xml, rels)

        # 이미지 추출 + (옵션) 설명
        if out_dir is None:
            out_dir = Path(path).parent / (Path(path).stem + ".images")
        extract_images_and_describe(z, blocks, out_dir, describe=describe)

    flat_text = build_flat_text(blocks, comments_meta)

    return {
        "blocks": blocks,
        "comments": comments_meta,
        "flat_text": flat_text,
        "stats": {
            "paragraph_count": sum(1 for b in blocks if b["type"] == "paragraph"),
            "table_count": sum(1 for b in blocks if b["type"] == "table"),
            "comment_count": len(comments_meta),
            "placeholder_count": sum(len(b.get("placeholders") or []) for b in blocks),
            "image_count": sum(
                len(b.get("images") or []) for b in blocks if b["type"] == "paragraph"
            ) + sum(
                len(cell.get("images") or [])
                for b in blocks if b["type"] == "table"
                for row in b.get("rows") or []
                for cell in row
            ),
            "flat_text_chars": len(flat_text),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    if not args:
        print("Usage: parse_docx_structured.py <input.docx> [--json | --text] [--describe] [--out-dir DIR]")
        sys.exit(1)
    path = args[0]
    json_mode = "--json" in args
    describe = "--describe" in args
    out_dir = None
    if "--out-dir" in args:
        idx = args.index("--out-dir")
        if idx + 1 < len(args):
            out_dir = Path(args[idx + 1])

    result = parse(path, out_dir=out_dir, describe=describe)
    if json_mode:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("=" * 80)
        print(f"FILE: {path}")
        print(f"STATS: {result['stats']}")
        print("=" * 80)
        print()
        print(result["flat_text"])
        if result["comments"]:
            print()
            print("=" * 80)
            print("COMMENTS INDEX")
            print("=" * 80)
            for cid, meta in result["comments"].items():
                print(f'  [{cid}] author={meta["author"]!r} date={meta["date"]!r}')
                print(f'       text: {meta["text"]}')


if __name__ == "__main__":
    main()
