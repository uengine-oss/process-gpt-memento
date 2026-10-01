"""HWPX 구조화 파서 — 텍스트 + 표(마크다운) + 이미지(VLM 설명) inline.

``vendor/extract_hwp/hwpx.py``(텍스트+표 전용)의 app 측 확장판이다.
이미지는 ``BinData/`` 에서 추출해 PDF·DOCX 와 동일한 공용 vision 헬퍼
(:mod:`app.plugins.parsers.vision`)로 설명을 받아 문서 흐름의 *해당 위치* 에
``[그림: ...]`` 으로 삽입한다.

  - 이미지 ID 매핑: ``Contents/content.hpf`` 의 ``<opf:item id=.. href=.. media-type=..>``
  - 섹션 내 참조 : ``<hp:pic>`` 안의 ``<hc:img binaryItemIDRef="..">``
  - LLM 설정     : ``resolve_llm_config()``(폐쇄망 custom 포함)
  - 동시 호출 수 : ``vision.VISION_MAX_WORKERS`` 상수
"""
from __future__ import annotations

import hashlib
import io
import sys
import zipfile
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple


def _local_tag(elem) -> str:
    """네임스페이스를 제거한 태그명."""
    t = elem.tag
    return t.split("}", 1)[1] if "}" in t else t


_SPACING_TAGS = {"tab", "lineBreak", "fwSpace", "nbSpace"}


def _t_text(node) -> str:
    """<t> 하나의 전체 텍스트 — <tab/> 등 인라인 요소 뒤의 tail 까지."""
    parts: List[str] = [node.text or ""]
    for child in node:
        if _local_tag(child) in _SPACING_TAGS:
            parts.append(" ")
        parts.append(child.tail or "")
    return "".join(parts)


# 머리말·꼬리말은 첫 문단의 조판 부호 안에 들어 있어 본문 글로 섞여 나온다.
_NOT_BODY = {"header", "footer"}


def _iter_body(elem, skip=_NOT_BODY):
    """하위 노드를 문서 순서로. skip 태그의 하위는 건너뛴다."""
    for child in elem:
        if _local_tag(child) in skip:
            continue
        yield child
        yield from _iter_body(child, skip)


def _collect_text(elem) -> str:
    """엘리먼트 하위의 모든 <t> 텍스트(표·머리말·꼬리말 제외)."""
    return "".join(_t_text(node) for node in _iter_body(elem, _NOT_BODY | {"tbl"})
                   if _local_tag(node) == "t")


def _parse_table_to_markdown(tbl_elem) -> str:
    """hp:tbl → 마크다운 표 (vendor/extract_hwp/hwpx.py 와 동일 로직)."""
    cells = []
    for tr in tbl_elem:
        if _local_tag(tr) != "tr":
            continue
        for tc in tr:
            if _local_tag(tc) != "tc":
                continue
            row = col = 0
            col_span = row_span = 1
            for cc in tc:
                tag = _local_tag(cc)
                if tag == "cellAddr":
                    for k, v in cc.attrib.items():
                        if "colAddr" in k:
                            col = int(v)
                        if "rowAddr" in k:
                            row = int(v)
                elif tag == "cellSpan":
                    for k, v in cc.attrib.items():
                        if "colSpan" in k:
                            try:
                                col_span = int(v)
                            except ValueError:
                                pass
                        if "rowSpan" in k:
                            try:
                                row_span = int(v)
                            except ValueError:
                                pass

            text_parts = []
            for sub in tc.iter():
                if _local_tag(sub) == "t":
                    text_parts.append(_t_text(sub))
            text = " ".join("".join(text_parts).split())
            cells.append((row, col, col_span, row_span, text))

    if not cells:
        return ""

    max_row = max(r + rs for r, c, cs, rs, t in cells)
    max_col = max(c + cs for r, c, cs, rs, t in cells)

    grid = [["" for _ in range(max_col)] for _ in range(max_row)]
    for row, col, col_span, row_span, text in cells:
        # 병합된 칸은 덮인 칸마다 같은 값을 둔다(떼어 읽은 행·열에도 머리가 남게)
        for r in range(row, min(row + row_span, max_row)):
            for c in range(col, min(col + col_span, max_col)):
                grid[r][c] = text

    lines = []
    for r in range(max_row):
        lines.append("| " + " | ".join(grid[r][c].replace("|", "\\|") or " " for c in range(max_col)) + " |")
        if r == 0:
            lines.append("| " + " | ".join(["---"] * max_col) + " |")

    return "\n".join(lines)


def _load_manifest(z: zipfile.ZipFile) -> Dict[str, Tuple[str, str]]:
    """content.hpf 의 opf:item → {id: (href, media_type)}."""
    out: Dict[str, Tuple[str, str]] = {}
    try:
        root = ET.fromstring(z.read("Contents/content.hpf"))
    except Exception:
        return out
    for item in root.iter():
        if _local_tag(item) != "item":
            continue
        iid = item.get("id")
        href = item.get("href")
        media = item.get("media-type") or ""
        if iid and href:
            out[iid] = (href, media)
    return out


def _normalize_for_vlm(data: bytes, mime: str) -> Tuple[bytes, str]:
    """일부 VLM 이 bmp/tiff 등을 못 받으므로 PNG 로 정규화(PIL 있으면). 실패 시 원본 유지."""
    if mime in ("image/png", "image/jpeg", "image/gif", "image/webp"):
        return data, mime
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(data))
        if img.mode not in ("RGB", "RGBA", "L"):
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, "PNG")
        return buf.getvalue(), "image/png"
    except Exception as exc:
        print(f"[hwpx] 이미지 PNG 변환 실패({mime}) → 원본 전송: {exc}", file=sys.stderr)
        return data, mime


def _load_image(
    z: zipfile.ZipFile, manifest: Dict[str, Tuple[str, str]], ref: str
) -> Optional[Tuple[bytes, str]]:
    """binaryItemIDRef → (bytes, mime). BinData 에서 추출 + VLM 용 정규화."""
    info = manifest.get(ref)
    href = info[0] if info else None
    media = info[1] if info else ""
    names = z.namelist()
    # 1) manifest href 그대로, 2) BinData 안에서 id 로 시작하는 파일 폴백
    candidates = []
    if href:
        candidates.append(href)
    candidates += [n for n in names if n.startswith("BinData/") and ref in n]
    for cand in candidates:
        try:
            data = z.read(cand)
        except KeyError:
            continue
        mime = media or _mime_from_name(cand)
        return _normalize_for_vlm(data, mime)
    return None


def _mime_from_name(name: str) -> str:
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    return {
        "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
        "gif": "image/gif", "bmp": "image/bmp", "tif": "image/tiff",
        "tiff": "image/tiff", "webp": "image/webp",
    }.get(ext, "application/octet-stream")


def _outline_levels(z: zipfile.ZipFile) -> Dict[str, int]:
    """header.xml 의 개요 수준 문단 모양 → {paraPr id: 1-based 헤딩 수준}."""
    try:
        root = ET.fromstring(z.read("Contents/header.xml"))
    except (KeyError, ET.ParseError):
        return {}
    levels: Dict[str, int] = {}
    for para_pr in root.iter():
        if _local_tag(para_pr) != "paraPr":
            continue
        for heading in para_pr:
            if _local_tag(heading) == "heading" and heading.get("type") == "OUTLINE":
                try:
                    levels[para_pr.get("id", "")] = int(heading.get("level", "0")) + 1
                except ValueError:
                    pass
    return levels


Token = Tuple[str, str, Optional[int]]


def _walk(elem, tokens: List[Token], outline: Dict[str, int]) -> None:
    """섹션 XML 을 문서 순서대로 순회 → [('text'|'table'|'image', value, heading_level)] 누적.

    image value 는 binaryItemIDRef. 표를 포함한 문단은 표 위치 보존을 위해 자식 재귀.
    """
    lt = _local_tag(elem)

    if lt == "tbl":
        md = _parse_table_to_markdown(elem)
        if md:
            tokens.append(("table", md, None))
        return

    if lt == "p":
        has_tbl = any(
            _local_tag(d) == "tbl" for d in elem.iter() if d is not elem
        )
        if has_tbl:
            # 표 포함 문단: 표 위치 보존 위해 자식 재귀
            for child in elem:
                _walk(child, tokens, outline)
        else:
            text = _collect_text(elem)
            if text.strip():
                tokens.append(("text", text, outline.get(elem.get("paraPrIDRef", ""))))
        # 이미지는 표 유무와 무관하게 문단에서 수집(문단 처리 뒤 삽입).
        # HWPX 는 이미지가 표와 같은 문단에 묶이는 경우가 흔하다.
        for sub in _iter_body(elem):
            if _local_tag(sub) == "img":
                ref = sub.get("binaryItemIDRef")
                if ref:
                    tokens.append(("image", ref, None))
        return

    for child in elem:
        _walk(child, tokens, outline)


def parse_blocks(path: str, describe: bool = True) -> List[Dict[str, Any]]:
    """HWPX → 문서 순서의 블록 목록 [{kind, text, heading_level}].

    kind 는 paragraph | table | image. describe=True 면 이미지를 공용 vision 헬퍼로 설명한다.
    LLM 미설정/실패 시 해당 이미지는 조용히 생략(fail-open).
    """
    with zipfile.ZipFile(path, "r") as z:
        manifest = _load_manifest(z)
        outline = _outline_levels(z)
        section_files = sorted(
            n for n in z.namelist()
            if n.startswith("Contents/section") and n.endswith(".xml")
        )
        if not section_files:
            raise ValueError("HWPX: section*.xml 을 찾지 못함")

        tokens: List[Token] = []
        for sec in section_files:
            _walk(ET.fromstring(z.read(sec)), tokens, outline)

        # 이미지 ref 수집(dedup) → 추출 → VLM 병렬 설명
        desc_map: Dict[str, str] = {}
        canon: Dict[str, str] = {}  # ref → 같은 내용의 첫 ref
        if describe:
            from app.plugins.parsers import vision  # 지연 import — app 의존성 격리

            ordered_refs: List[str] = []
            seen = set()
            for kind, val, _level in tokens:
                if kind == "image" and val not in seen:
                    seen.add(val)
                    ordered_refs.append(val)
            images: Dict[str, Tuple[bytes, str]] = {}
            by_hash: Dict[str, str] = {}
            for ref in ordered_refs:
                loaded = _load_image(z, manifest, ref)
                if not loaded:
                    print(f"[hwpx] 이미지 추출 실패: ref={ref}", file=sys.stderr)
                    continue
                if not vision.worth_describing(loaded[0]):
                    continue
                # 같은 그림이 BinData 항목 여러 개로 들어 있어도(반복 로고 등) 한 번만 설명한다
                canon[ref] = by_hash.setdefault(hashlib.sha1(loaded[0]).hexdigest(), ref)
                if canon[ref] == ref:
                    images[ref] = loaded
            if images:
                print(
                    f"[vision] HWPX {len(images)}개 이미지 병렬 처리 시작",
                    file=sys.stderr,
                )
                tasks = [
                    (
                        ref,
                        (lambda d=data, m=mime: vision.describe_image(d, mime_type=m)),
                    )
                    for ref, (data, mime) in images.items()
                ]
                desc_map = vision.run_parallel(tasks)

    blocks: List[Dict[str, Any]] = []
    placed = set()
    for kind, val, level in tokens:
        if kind == "image":
            ref = canon.get(val)
            if ref is None or ref in placed:
                continue  # 설명 안 한 그림, 또는 이미 앞에서 설명을 넣은 같은 그림
            placed.add(ref)
            desc = (desc_map.get(ref) or "").strip()
            if desc:
                blocks.append({"kind": "image", "text": f"[그림: {desc}]", "heading_level": None})
        else:
            blocks.append({
                "kind": "paragraph" if kind == "text" else "table",
                "text": val,
                "heading_level": level,
            })
    return blocks


def parse(path: str, describe: bool = True) -> str:
    """HWPX → 텍스트(+표 마크다운 + 이미지 VLM 설명 inline)."""
    return "\n\n".join(block["text"] for block in parse_blocks(path, describe=describe))
