"""PyMuPDF **region** 파서 — 이미지 XObject 단위가 아니라 *시각 영역(region)* 단위로 VLM 처리.

배경(딥리서치 근거): 문서 파싱의 성숙한 패턴은 "per-object(XObject)마다 VLM 호출"이 아니라
레이아웃/영역 단위 라우팅 + 중복 병합(redundancy-resilient merging)이다. 한 도식이 수십 개
타일 XObject 로 쪼개진 PDF 에서 기존 pymupdf 전략은 조각 수만큼 VLM 을 부르는 **호출 폭발**이
생긴다(픽셀·개수 임계치로 사후 정리해도 근본 해결 안 됨).

이 전략은 페이지를 3-way 로 처리한다(vision on 기준):
  1. 텍스트만            → 파서 텍스트+표 추출 (VLM 0)                         [부모와 동일]
  2. 텍스트 레이어 없음  → 페이지 전체 렌더 → 통합 OCR                          [부모와 동일]
  3. 텍스트 + 삽입 이미지 → ★ 이미지 placement rect 들을 *기하학적으로 병합* 해
                           "그림 영역" 을 만들고, 각 영역을 페이지에서 크롭해
                           **영역당 VLM 1회** (describe/ocr). ← 폭발 제거 지점

핵심: VLM 호출 수가 *XObject 수* 가 아니라 *병합된 그림 영역 수* 에 비례한다.
튜닝값은 픽셀 절대치가 아니라 **페이지 대비 상대 비율** 로만 두어(임의성 최소화), degrade 가
bounded 하도록 한다(잘못돼도 "1영역이 2영역으로 나뉘는" 정도, 30개 폭발 아님).

env ``PDF_STRATEGY=pymupdf_region`` 으로 선택. 기본(pymupdf)은 그대로 두어 사이드이펙트 0 / A-B 비교.
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict, List, Tuple

from langchain.schema import Document

from . import vision
from .pymupdf_parser import PyMuPDFParser


class PyMuPDFRegionParser(PyMuPDFParser):
    name = "pymupdf_region"
    supported_extensions = (".pdf",)

    # ── 영역 파라미터 (모두 페이지 대비 *상대 비율* — 절대 픽셀 임계치 지양) ──
    #: 두 이미지 rect 가 페이지 짧은 변의 이 비율 이내로 붙어 있으면 *같은 그림* 으로 병합.
    #: 기하학적 인접성이라 80px 같은 임의값보다 견고. 잘못돼도 영역이 조금 더/덜 뭉칠 뿐.
    _MERGE_GAP_FRAC = 0.012
    #: 병합된 영역 면적이 페이지의 이 비율 미만이면 아이콘/장식으로 보고 스킵(영역 단위 필터).
    _MIN_REGION_AREA_FRAC = 0.01
    #: 영역이 페이지의 이 비율 이상을 덮으면 캡션(describe) 대신 통합 OCR 프롬프트 사용
    #: (큰 영역 = 밀도 높은 내용/준-스캔 → 원문 받아쓰기가 더 정확).
    _REGION_OCR_COVERAGE = 0.5
    #: 영역이 페이지를 거의 통째로 덮는데 *페이지에 텍스트 레이어가 있으면*, 배경/워터마크/
    #: 레터헤드로 보고 스킵(텍스트는 이미 파서가 뽑았으니 재-OCR 은 노이즈만). 상대 비율 밴드.
    _SKIP_BG_COVERAGE = 0.95
    #: 영역 크롭 렌더 해상도(dpi). 부모의 모듈 상수와 동일값을 *클래스 속성* 으로 둔다
    #: (부모 _render_page_png 는 모듈 상수를 직접 참조하지만, region 크롭은 self._RENDER_DPI 사용).
    #: 낮추면 VLM 에 가는 이미지 토큰↓ → 비전 서버 VRAM 부담↓ (OOM 완화 레버).
    _RENDER_DPI = 200

    def _parse_sync(self, file_content: bytes, file_name: str) -> List[Document]:
        import fitz  # PyMuPDF

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(file_content)
            tmp_path = tmp.name

        vision_on = vision.pdf_vision_enabled()
        docs: List[Document] = []
        try:
            pdf = fitz.open(tmp_path)

            # ── 1차 패스: 텍스트/영역 수집 + VLM 작업 등록 ──────────────────────
            page_infos: List[Dict[str, Any]] = []
            tasks: List[Tuple[str, Any]] = []  # (key, thunk)
            for page_num, page in enumerate(pdf):
                text_items, page_size = self._text_items(page)
                has_text = bool(text_items)
                page_w, page_h = page_size
                page_area = (page_w * page_h) or 1.0

                info: Dict[str, Any] = {
                    "page_num": page_num, "page_size": page_size,
                    "text_items": text_items, "regions": [], "ocr_key": None, "mode": "text",
                }

                if vision_on and not has_text:
                    # 2번: 텍스트 레이어 없음 → 페이지 전체 OCR (부모와 동일).
                    if self._has_raw_image(page) or self._has_vector(page):
                        png = self._render_page_png(page)
                        if png:
                            key = f"ocr:{page_num}"
                            info["ocr_key"] = key
                            info["mode"] = "ocr"
                            tasks.append((key, (lambda b=png: vision.ocr_page_image(b))))
                elif vision_on and has_text:
                    # 3번: 텍스트 + 이미지 → ★ 이미지 rect 병합 → 영역당 1회.
                    regions = self._figure_regions(page, page_w, page_h, page_area)
                    if regions:
                        info["mode"] = "region"
                        for idx, reg in enumerate(regions):
                            png = self._render_clip_png(page, reg["bbox"], self._RENDER_DPI)
                            if not png:
                                continue
                            key = f"reg:{page_num}:{idx}"
                            reg["key"] = key
                            is_ocr = reg["coverage"] >= self._REGION_OCR_COVERAGE
                            reg["is_ocr"] = is_ocr
                            info["regions"].append(reg)
                            if is_ocr:
                                tasks.append((key, (lambda b=png: vision.ocr_page_image(b))))
                            else:
                                tasks.append((key, (lambda b=png: vision.describe_image(b, "image/png"))))

                page_infos.append(info)

            if tasks:
                print(f"[vision] PDF(region) '{file_name}' VLM 처리 시작")
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
                    entries = list(info["text_items"])  # [(y, text, bbox)]
                    used = 0
                    for reg in info["regions"]:
                        out = (results.get(reg.get("key", "")) or "").strip()
                        if not out:
                            continue
                        # 큰 영역(OCR)은 본문 그대로, 작은 영역(캡션)은 [그림: ...] 로 표시.
                        text = out if reg.get("is_ocr") else f"[그림: {out}]"
                        entries.append((reg["y"], text, reg["bbox"]))
                        used += 1
                    markdown, blocks = self._build_markdown(page_num, entries)
                    if used:
                        meta_extra["vision_regions"] = used

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

    # ── 영역(region) 도출 ──────────────────────────────────────────────────────

    def _figure_regions(
        self, page, page_w: float, page_h: float, page_area: float
    ) -> List[Dict[str, Any]]:
        """페이지의 이미지 placement rect 들을 병합해 '그림 영역' 목록을 만든다.

        반환: [{"bbox":[x0,y0,x1,y1], "y":float, "coverage":float}] (y정렬).
        """
        rects = self._image_placement_rects(page)
        if not rects:
            return []
        gap = self._MERGE_GAP_FRAC * min(page_w, page_h)
        merged = self._merge_boxes(rects, gap)

        out: List[Dict[str, Any]] = []
        for b in merged:
            w = max(0.0, b[2] - b[0])
            h = max(0.0, b[3] - b[1])
            area_frac = (w * h) / page_area
            if area_frac < self._MIN_REGION_AREA_FRAC:
                continue  # 아이콘/장식 (영역 단위 필터, per-object 아님)
            if area_frac >= self._SKIP_BG_COVERAGE:
                continue  # 텍스트 페이지의 전면 배경/워터마크 → 스킵(텍스트는 이미 있음)
            out.append({"bbox": [b[0], b[1], b[2], b[3]], "y": b[1], "coverage": area_frac})
        out.sort(key=lambda r: r["y"])
        return out

    @staticmethod
    def _image_placement_rects(page) -> List[List[float]]:
        """페이지에 배치된 모든 이미지의 사각형(placement rect) 수집.

        XObject 를 *뽑는* 게 아니라 *페이지 상 위치* 를 모은다 → 병합의 입력.
        같은 이미지가 여러 번 배치되면 각 배치가 rect 1개. 1px 이하 노이즈는 제외.
        """
        rects: List[List[float]] = []
        try:
            images = page.get_images(full=True)
        except Exception:
            return rects
        seen: set = set()
        for img in images:
            xref = img[0]
            if xref in seen:
                continue
            seen.add(xref)
            try:
                for r in page.get_image_rects(xref):
                    if (r.x1 - r.x0) <= 1 or (r.y1 - r.y0) <= 1:
                        continue
                    rects.append([float(r.x0), float(r.y0), float(r.x1), float(r.y1)])
            except Exception:
                continue
        return rects

    @staticmethod
    def _merge_boxes(boxes: List[List[float]], gap: float) -> List[List[float]]:
        """겹치거나 ``gap`` 이내로 인접한 bbox 들을 connected-component 로 병합.

        안정될 때까지 반복(작은 페이지당 이미지 수라 O(n^2) 로 충분). 결과=각 그룹의 합집합 bbox.
        """
        boxes = [list(b) for b in boxes]
        changed = True
        while changed:
            changed = False
            i = 0
            while i < len(boxes):
                j = i + 1
                while j < len(boxes):
                    if PyMuPDFRegionParser._boxes_near(boxes[i], boxes[j], gap):
                        boxes[i] = [
                            min(boxes[i][0], boxes[j][0]), min(boxes[i][1], boxes[j][1]),
                            max(boxes[i][2], boxes[j][2]), max(boxes[i][3], boxes[j][3]),
                        ]
                        boxes.pop(j)
                        changed = True
                    else:
                        j += 1
                i += 1
        return boxes

    @staticmethod
    def _boxes_near(a: List[float], b: List[float], gap: float) -> bool:
        """a 를 gap 만큼 확장했을 때 b 와 겹치는가(=인접/중첩)."""
        return not (
            a[2] + gap < b[0] or a[0] - gap > b[2] or a[3] + gap < b[1] or a[1] - gap > b[3]
        )

    @staticmethod
    def _render_clip_png(page, bbox: List[float], dpi: int):
        """페이지에서 bbox 영역만 크롭 렌더 → PNG bytes. 실패 시 None."""
        import fitz
        try:
            zoom = dpi / 72.0
            clip = fitz.Rect(bbox[0], bbox[1], bbox[2], bbox[3])
            pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=clip, alpha=False)
            return pix.tobytes("png")
        except Exception as exc:  # noqa: BLE001
            print(f"[vision] region 크롭 렌더 실패 bbox={bbox}: {exc}")
            return None
