"""PyMuPDF(fitz) 기반 PDF 파서. 페이지 단위 스트리밍, 표는 y좌표 기반 정렬.

vision(MEMENTO_PDF_VISION, 기본 on) 활성화 시 페이지를 3가지로 처리:
  1. 텍스트만           → 파서로 텍스트+표 추출 (VLM 호출 없음)
  2. 이미지만(텍스트 X) → 페이지 전체 렌더 → VLM 통합 OCR(+`[도식: ...]`)
  3. 텍스트 + 이미지    → 텍스트는 파서로, 각 이미지는 VLM 으로 설명한 뒤
                          *이미지가 있던 y 위치* 에 `[그림: ...]` 으로 inline 삽입
토글이 꺼져 있으면 1번만 동작(텍스트 없는 페이지는 빈 본문 — 기존 동작).
"""
import os
import tempfile
import asyncio
import json
from typing import List, Dict, Any, Optional, Tuple
from langchain.schema import Document

from .base import BaseParser
from . import vision


# 본문 삽입 그림 필터: 너무 작은 로고/아이콘, 페이지 전면 배경은 설명 대상에서 제외.
_IMG_MIN_PX = 80
_IMG_MAX_COVERAGE = 0.9
# 스캔 페이지 렌더 해상도(dpi).
_RENDER_DPI = 200


def _intersects(a, b) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


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

            # ── 1차 패스: 텍스트/이미지 수집 + VLM 작업 등록 ───────────────────
            page_infos: List[Dict[str, Any]] = []
            tasks: List[Tuple[str, Any]] = []   # (key, thunk)
            for page_num, page in enumerate(pdf):
                text_items, page_size = self._text_items(page)
                has_text = bool(text_items)

                info: Dict[str, Any] = {
                    "page_num": page_num, "page_size": page_size,
                    "text_items": text_items, "img_items": [],
                    "ocr_key": None, "mode": "text",
                }

                if vision_on and not has_text:
                    # 2번: 이미지만(텍스트 레이어 없음) → 페이지 전체 OCR.
                    # 트리거는 *필터 전* raw 이미지/벡터 유무 (전면 스캔 이미지도 포함).
                    if self._has_raw_image(page) or self._has_vector(page):
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
                            key = f"img:{page_num}:{it['xref']}"
                            it["key"] = key
                            tasks.append(
                                (key, (lambda b=it["bytes"], m=it["mime"]: vision.describe_image(b, m)))
                            )

                page_infos.append(info)

            if tasks:
                print(f"[vision] PDF '{file_name}' VLM {len(tasks)}건 처리 시작")
            results = vision.run_parallel(tasks) if tasks else {}

            # ── 2차 패스: Document 조립 ───────────────────────────────────────
            for info in page_infos:
                page_num = info["page_num"]
                page_size = info["page_size"]
                meta_extra: Dict[str, Any] = {}

                if info["mode"] == "ocr":
                    ocr_text = (results.get(info["ocr_key"]) or "").strip()
                    markdown = f"# 페이지 {page_num + 1}\n\n{ocr_text}" if ocr_text else ""
                    blocks: list = []
                    if ocr_text:
                        meta_extra["vision_ocr"] = True
                else:
                    # text(1번) / mixed(3번) 공통: y정렬 엔트리로 조립
                    entries = list(info["text_items"])  # [(y, text, bbox)]
                    used = 0
                    for it in info["img_items"]:
                        desc = (results.get(it.get("key", "")) or "").strip()
                        if desc:
                            entries.append((it["y"], f"[그림: {desc}]", it["bbox"]))
                            used += 1
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
    def _text_items(page) -> Tuple[List[Tuple[float, str, list]], Tuple[float, float]]:
        """페이지의 텍스트 블록 + 표를 [(y_sort_key, text, bbox)] 로 수집(정렬 전)."""
        valid_tables = []
        try:
            for tab in page.find_tables().tables:
                if not tab.cells:
                    continue
                try:
                    _ = tab.bbox
                except ValueError:
                    continue
                valid_tables.append(tab)
        except Exception:
            pass

        table_bboxes = [t.bbox for t in valid_tables]
        items: List[Tuple[float, str, list]] = []

        for block in page.get_text("blocks"):
            bbox, text = block[:4], block[4]
            if any(_intersects(bbox, tb) for tb in table_bboxes):
                continue
            stripped = text.strip()
            if stripped:
                items.append((bbox[1], stripped, list(bbox)))

        for tab in valid_tables:
            try:
                items.append((tab.bbox[1], tab.to_markdown(), list(tab.bbox)))
            except Exception:
                continue

        rect = page.rect
        return items, (float(rect.width), float(rect.height))

    @staticmethod
    def _image_items(doc, page) -> List[Dict[str, Any]]:
        """본문 삽입 그림 raster 추출 + y위치. 로고/아이콘·전면배경 제외, xref 중복 제거.

        반환: [{"xref", "bytes", "mime", "y", "bbox"}].
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
                # y위치 + 커버리지
                y = 1e9
                bbox = [0.0, 0.0, 0.0, 0.0]
                coverage = 0.0
                try:
                    rects = page.get_image_rects(xref)
                    if rects:
                        r = max(rects, key=lambda rr: rr.width * rr.height)
                        y = float(r.y0)
                        bbox = [float(r.x0), float(r.y0), float(r.x1), float(r.y1)]
                        coverage = (r.width * r.height) / page_area
                except Exception:
                    pass
                if coverage >= _IMG_MAX_COVERAGE:
                    continue  # 전면 배경/스캔 → 그림 설명 대상 아님

                base = doc.extract_image(xref)
                if base.get("width", 0) < _IMG_MIN_PX or base.get("height", 0) < _IMG_MIN_PX:
                    continue
                data = base.get("image") or b""
                if not data:
                    continue
                ext = (base.get("ext") or "png").lower()
                mime = {
                    "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
                    "gif": "image/gif", "bmp": "image/bmp", "webp": "image/webp",
                }.get(ext, "image/png")
                out.append({"xref": xref, "bytes": data, "mime": mime, "y": y, "bbox": bbox})
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
        """[(y, text, bbox)] 를 y정렬해 markdown + blocks(offset/length/bbox) 생성."""
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
