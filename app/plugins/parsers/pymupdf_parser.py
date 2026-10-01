"""PyMuPDF(fitz) 기반 PDF 파서. 페이지 단위 스트리밍.

읽기 순서는 PDF 에 기록된 글 순서를 따르고, 표·그림은 그 순서 안의 제자리에 끼운다
(근거: docs/DESIGN_NOTES.md#pdf-읽기-순서).

vision(MEMENTO_PDF_VISION, 기본 on) 활성화 시 페이지를 3가지로 처리:
  1. 텍스트만           → 파서로 텍스트+표 추출 (VLM 호출 없음)
  2. 이미지만(텍스트 X) → 페이지 전체 렌더 → VLM 통합 OCR(+`[도식: ...]`)
  3. 텍스트 + 이미지    → 텍스트는 파서로, 각 이미지는 VLM 으로 설명한 뒤
                          *이미지가 있던 자리* 에 `[그림: ...]` 으로 inline 삽입
토글이 꺼져 있으면 1번만 동작(텍스트 없는 페이지는 빈 본문 — 기존 동작).
"""
import os
import tempfile
import asyncio
import json
import re
import unicodedata
from collections import Counter
from typing import List, Dict, Any, Optional, Tuple
from langchain.schema import Document

from .base import BaseParser
from . import config, unruled_tables, vision
from .subrows import split_subrows


# 본문 삽입 그림 필터: 너무 작은 로고/아이콘, 페이지 전면 배경은 설명 대상에서 제외.
_IMG_MIN_PX = vision.IMG_MIN_PX
_IMG_MAX_COVERAGE = 0.9
# 스캔 페이지 렌더 해상도(dpi).
_RENDER_DPI = 200
# 머리말·꼬리말 후보: 쪽 높이의 위·아래 이 비율 안에 있는 글.
_MARGIN_BAND = 0.1


def margin_key(block, page_height: float) -> Optional[str]:
    """쪽 가장자리 글의 반복 비교용 키(숫자는 같은 것으로 본다 — 쪽 번호). 가장자리가 아니면 None."""
    if block[3] > page_height * _MARGIN_BAND and block[1] < page_height * (1 - _MARGIN_BAND):
        return None
    key = re.sub(r"\d+", "#", " ".join(str(block[4]).split()))
    return key or None


def repeated_margin_text(pdf) -> set:
    """여러 쪽 위·아래 가장자리에 되풀이되는 글(머리말·꼬리말·쪽 번호)의 키."""
    total = pdf.page_count
    if total < 3:
        return set()
    pages: Dict[str, set] = {}
    for pno, page in enumerate(pdf):
        height = page.rect.height
        for block in page.get_text("blocks"):
            key = margin_key(block, height)
            if key:
                pages.setdefault(key, set()).add(pno)
    need = max(3, total // 2)
    return {k for k, ps in pages.items() if len(ps) >= need}


def _center_inside(b, box, tol: float = 1.0) -> bool:
    cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
    return box[0] - tol <= cx <= box[2] + tol and box[1] - tol <= cy <= box[3] + tol


_KNOWN_SCRIPTS = ("HANGUL", "LATIN", "CJK UNIFIED", "CJK COMPATIBILITY IDEOGRAPH", "HIRAGANA", "KATAKANA",
                  "GREEK", "CYRILLIC")


def garbled_text(text: str) -> bool:
    """글꼴 대응표가 없는 PDF 의 텍스트 레이어처럼 글자가 엉뚱한 문자로 나오는가.

    깨진 글은 글자가 여러 문자 체계로 흩어지고, 흔히 쓰는 문자 체계(한글·라틴·한자·가나 등)가 적다.
    둘 다일 때만 깨진 것으로 본다(아랍어처럼 한 문자 체계로 모인 글, 일본어처럼 흔한 체계끼리 섞인 글은 아니다).
    근거: docs/DESIGN_NOTES.md#파서-점검
    """
    letters = [c for c in text if unicodedata.category(c).startswith("L")]
    if len(letters) < 30:
        return False
    names = [unicodedata.name(c, "?") for c in letters]
    known = sum(n.startswith(_KNOWN_SCRIPTS) for n in names) / len(letters)
    scripts = Counter(n.split(" ")[0] for n in names)
    top2 = sum(v for _, v in scripts.most_common(2)) / len(letters)
    return known < 0.5 and top2 < 0.8


def _md_columns(line: str) -> int:
    return line.strip().strip("|").count("|") + 1


def carry_table_header(entries: List[Tuple[float, str, list]], prev_table: Optional[str]):
    """쪽을 넘긴 표에 앞 쪽 표의 머리행을 다시 붙인다. (entries, 이 쪽 마지막 요소가 표면 그 글) 을 돌려준다.

    앞 쪽이 표로 끝나고 이 쪽이 열 수가 같은 표로 시작하면 이어지는 표로 본다. 머리행을 반복하지 않은
    표는 둘째 쪽 첫 행이 머리행 자리에 들어가 열 이름을 잃는다.
    """
    ordered = sorted(entries, key=lambda e: e[0])
    if ordered and prev_table and ordered[0][1].startswith("|"):
        head = prev_table.splitlines()[:2]
        cur = ordered[0][1].splitlines()
        if len(head) == 2 and len(cur) >= 2 and _md_columns(head[0]) == _md_columns(cur[0]) and head[0] != cur[0]:
            body = [cur[0]] + cur[2:]  # 제 머리행 자리의 첫 행은 본문으로 내린다
            ordered[0] = (ordered[0][0], "\n".join(head + body), ordered[0][2])
    last = ordered[-1][1] if ordered and ordered[-1][1].startswith("|") else None
    return ordered, last


def _bbox_key(bbox) -> tuple:
    return tuple(round(float(v), 2) for v in bbox[:4])


def _x_overlap(a, b) -> bool:
    return a[0] < b[2] and b[0] < a[2]


def place_key(entries: List[Tuple[float, str, list]], bbox) -> float:
    """글 순서 목록 안에서 표·그림이 들어갈 자리(정렬 키).

    같은 단(가로로 겹치는)에서 바로 위 블록 뒤, 없으면 바로 아래 블록 앞, 둘 다 없으면 세로 위치로.
    """
    above = [e for e in entries if e[2][3] <= bbox[1] + 2 and _x_overlap(e[2], bbox)]
    if above:
        return max(above, key=lambda e: e[2][3])[0] + 0.5 + bbox[1] / 1e6
    below = [e for e in entries if e[2][1] >= bbox[3] - 2 and _x_overlap(e[2], bbox)]
    if below:
        return min(below, key=lambda e: e[2][1])[0] - 0.5 + bbox[1] / 1e6
    later = [e[0] for e in entries if e[2][1] > bbox[1]]
    return (min(later) if later else len(entries)) - 0.5 + bbox[1] / 1e6


class PyMuPDFParser(BaseParser):
    name = "pymupdf"
    supported_extensions = (".pdf",)

    async def parse(self, file_content: bytes, file_name: str) -> List[Document]:
        return await asyncio.to_thread(self._parse_sync, file_content, file_name)

    def _parse_sync(self, file_content: bytes, file_name: str) -> List[Document]:
        import fitz  # PyMuPDF

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(file_content)
            tmp_path = tmp.name

        vision_on = vision.pdf_vision_enabled()
        docs: List[Document] = []
        try:
            pdf = fitz.open(tmp_path)
            margins = repeated_margin_text(pdf)

            # ── 1차 패스: 텍스트/이미지 수집 + VLM 작업 등록 ───────────────────
            page_infos: List[Dict[str, Any]] = []
            tasks: List[Tuple[str, Any]] = []   # (key, thunk)
            requested: set = set()
            for page_num, page in enumerate(pdf):
                text_items, page_size = self._text_items(page, margins)
                # 깨진 텍스트 레이어는 글이 없는 쪽처럼 OCR 로 보낸다
                garbled = bool(text_items) and garbled_text(page.get_text())
                if garbled:
                    text_items = []
                has_text = bool(text_items)

                info: Dict[str, Any] = {
                    "page_num": page_num, "page_size": page_size,
                    "text_items": text_items, "img_items": [],
                    "ocr_key": None, "mode": "text",
                }

                if vision_on and not has_text:
                    # 2번: 이미지만(텍스트 레이어 없음) → 페이지 전체 OCR.
                    # 트리거는 *필터 전* raw 이미지/벡터 유무 (전면 스캔 이미지도 포함).
                    if garbled or self._has_raw_image(page) or self._has_vector(page):
                        png = self._render_page_png(page)
                        if png:
                            key = f"ocr:{page_num}"
                            info["ocr_key"] = key
                            info["mode"] = "ocr"
                            tasks.append((key, (lambda b=png: vision.ocr_page_image(b))))
                elif vision_on and has_text:
                    # 3번: 텍스트 + (삽입)이미지 → 이미지별 설명 (자리 보존 삽입).
                    # img_items 는 로고/전면배경 제외된 *본문 삽입 그림* 만.
                    img_items = self._image_items(pdf, page)
                    if img_items:
                        info["img_items"] = img_items
                        info["mode"] = "mixed"
                        for it in img_items:
                            # 같은 그림(xref)은 문서에서 한 번만 설명한다
                            key = f"img:{it['xref']}"
                            it["key"] = key
                            if key not in requested:
                                requested.add(key)
                                tasks.append(
                                    (key, (lambda b=it["bytes"], m=it["mime"]: vision.describe_image(b, m)))
                                )

                page_infos.append(info)

            if tasks:
                print(f"[vision] PDF '{file_name}' VLM {len(tasks)}건 처리 시작")
            results = vision.run_parallel(tasks) if tasks else {}

            # ── 2차 패스: Document 조립 ───────────────────────────────────────
            prev_table = None  # 앞 쪽이 표로 끝났으면 그 표
            for info in page_infos:
                page_num = info["page_num"]
                page_size = info["page_size"]
                meta_extra: Dict[str, Any] = {}

                if info["mode"] == "ocr":
                    prev_table = None
                    ocr_text = (results.get(info["ocr_key"]) or "").strip()
                    markdown = f"# 페이지 {page_num + 1}\n\n{ocr_text}" if ocr_text else ""
                    blocks: list = []
                    if ocr_text:
                        meta_extra["vision_ocr"] = True
                else:
                    # text(1번) / mixed(3번) 공통: 글 순서 안의 제자리에 그림 설명을 끼운다
                    entries = list(info["text_items"])  # [(순서 키, text, bbox)]
                    used = 0
                    for it in info["img_items"]:
                        desc = (results.get(it.get("key", "")) or "").strip()
                        if desc:
                            entries.append((place_key(info["text_items"], it["bbox"]), f"[그림: {desc}]", it["bbox"]))
                            used += 1
                    entries, prev_table = carry_table_header(entries, prev_table)
                    markdown, blocks = self._build_markdown(page_num, entries)
                    if used:
                        meta_extra["vision_images"] = used

                metadata = {
                    "source": file_name,
                    "page": page_num,
                    "blocks_json": json.dumps(blocks, ensure_ascii=False) if blocks else "",
                    "page_width": page_size[0],
                    "page_height": page_size[1],
                }
                metadata.update(meta_extra)
                docs.append(Document(page_content=markdown, metadata=metadata))

            pdf.close()
        finally:
            os.unlink(tmp_path)

        return self._tag(docs)

    # ── 수집 보조 ─────────────────────────────────────────────────────────────

    @staticmethod
    def _tables(page) -> List[Tuple[list, str]]:
        """[(bbox, markdown)]. 두 칸 이상에 글이 없는 '표'는 선·글자 도형을 표로 오인한 것이라 버린다."""
        out: List[Tuple[list, str]] = []
        try:
            found = page.find_tables().tables
        except Exception:
            return out
        for tab in found:
            try:
                cells = [c for row in tab.extract() for c in row if c and str(c).strip()]
                # 한 행짜리도 받는다: 한 표를 행마다 따로 잡는 PDF 가 있다
                if tab.col_count < 2 or len(cells) < 2:
                    continue
                md = tab.to_markdown()
                if config.PDF_SPLIT_SUBROWS:
                    md = split_subrows(page, tab, md)
                out.append((list(tab.bbox), md))
            except Exception:
                continue
        return out

    @staticmethod
    def _text_items(page, skip: Optional[set] = None) -> Tuple[List[Tuple[float, str, list]], Tuple[float, float]]:
        """페이지의 글 블록 + 표를 [(순서 키, text, bbox)] 로. 순서 키는 PDF 에 기록된 글 순서다.

        ``skip`` 은 repeated_margin_text() 의 키(되풀이되는 머리말·꼬리말)로, 그 글은 뺀다.
        """
        tables = PyMuPDFParser._tables(page)
        lines_by_block = {}
        if config.PDF_UNRULED_TABLES:
            # blocks 와 dict 는 블록 번호가 다르게 매겨지는 PDF 가 있어 좌표로 짝짓는다(번호로 짝지으면 줄이 엉뚱한 블록에 붙었다)
            lines_by_block = {_bbox_key(b["bbox"]): b for b in page.get_text("dict")["blocks"] if b.get("type") == 0}
        entries: List[Dict[str, Any]] = []
        placed = set()
        for idx, block in enumerate(page.get_text("blocks")):
            if len(block) > 6 and block[6] != 0:
                continue  # 그림 블록
            bbox, text = list(block[:4]), block[4].strip()
            if not text or (skip and margin_key(block, page.rect.height) in skip):
                continue
            # 가운데가 표 안인 글만 표의 몫이다(가장자리만 걸친 제목·출처 줄은 살린다)
            owner = next((k for k, (tb, _) in enumerate(tables) if _center_inside(bbox, tb)), None)
            if owner is None:
                cells = unruled_tables.line_cells(lines_by_block.get(_bbox_key(bbox))) if lines_by_block else None
                entries.append({"key": float(idx), "text": text, "bbox": bbox, "cells": cells})
            elif owner not in placed:
                placed.add(owner)
                entries.append({"key": float(idx), "text": tables[owner][1], "bbox": tables[owner][0], "cells": None})
        if lines_by_block:
            entries = unruled_tables.rebuild(entries)
        items: List[Tuple[float, str, list]] = [(e["key"], e["text"], e["bbox"]) for e in entries]
        for k, (tb, md) in enumerate(tables):
            if k not in placed:
                items.append((place_key(items, tb), md, tb))

        rect = page.rect
        return items, (float(rect.width), float(rect.height))

    @staticmethod
    def _image_items(doc, page) -> List[Dict[str, Any]]:
        """본문 삽입 그림 raster 추출 + 위치. 로고/아이콘·전면배경 제외, xref 중복 제거.

        반환: [{"xref", "bytes", "mime", "bbox"}]. 쪽에 실제로 그려진 그림만 낸다
        (여러 쪽이 그림 목록을 공유하는 PDF 는 안 그려진 쪽에도 목록에 나온다).
        """
        out: List[Dict[str, Any]] = []
        seen: set = set()
        try:
            page_area = float(page.rect.width) * float(page.rect.height) or 1.0
        except Exception:
            page_area = 1.0
        try:
            image_list = page.get_images(full=True)
        except Exception:
            return out

        for img in image_list:
            xref = img[0]
            if xref in seen:
                continue
            seen.add(xref)
            try:
                try:
                    rects = [r for r in page.get_image_rects(xref) if r.width > 1 and r.height > 1]
                except Exception:
                    rects = []
                if not rects:
                    continue
                r = max(rects, key=lambda rr: rr.width * rr.height)
                bbox = [float(r.x0), float(r.y0), float(r.x1), float(r.y1)]
                coverage = (r.width * r.height) / page_area
                if coverage >= _IMG_MAX_COVERAGE:
                    continue  # 전면 배경/스캔 → 그림 설명 대상 아님

                base = doc.extract_image(xref)
                if base.get("width", 0) < _IMG_MIN_PX or base.get("height", 0) < _IMG_MIN_PX:
                    continue
                data = base.get("image") or b""
                if not data:
                    continue
                ext = (base.get("ext") or "png").lower()
                mime = vision.guess_image_mime(ext)
                out.append({"xref": xref, "bytes": data, "mime": mime, "bbox": bbox})
            except Exception as exc:
                print(f"[vision] 이미지 xref={xref} 추출 실패: {exc}")
                continue
        return out

    @staticmethod
    def _has_raw_image(page) -> bool:
        """필터 이전, 페이지에 이미지(전면 스캔 포함)가 하나라도 있는지."""
        try:
            return bool(page.get_images())
        except Exception:
            return False

    @staticmethod
    def _has_vector(page) -> bool:
        """텍스트·raster 없이 벡터 도형만 있는 페이지 감지(빈 페이지 OCR 낭비 방지용)."""
        try:
            return bool(page.get_drawings())
        except Exception:
            return False

    @staticmethod
    def _render_page_png(page) -> Optional[bytes]:
        """스캔 페이지를 PNG 로 렌더링. 해상도는 상수 _RENDER_DPI."""
        import fitz
        try:
            zoom = _RENDER_DPI / 72.0
            pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
            return pix.tobytes("png")
        except Exception as exc:
            print(f"[vision] 페이지 렌더 실패: {exc}")
            return None

    @staticmethod
    def _build_markdown(page_num: int, entries: List[Tuple[float, str, list]]):
        """[(순서 키, text, bbox)] 를 키 순서로 markdown + blocks(offset/length/bbox) 생성."""
        entries = sorted(entries, key=lambda e: e[0])
        header = f"# 페이지 {page_num + 1}\n\n"
        parts = [header]
        cursor = len(header)
        blocks: list = []
        for idx, (_y, text, bbox) in enumerate(entries):
            if idx > 0:
                parts.append("\n\n")
                cursor += 2
            blocks.append({"offset": cursor, "length": len(text), "bbox": bbox})
            parts.append(text)
            cursor += len(text)
        markdown = "".join(parts) if entries else ""
        return markdown, blocks
