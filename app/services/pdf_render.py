"""PDF 쪽 그림·강조·삽입 그림 추출 (pypdfium2). 좌표는 PDF 포인트, 왼쪽 위 원점."""
from __future__ import annotations

import io
from typing import Iterator, Optional, Sequence, Tuple

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
from PIL import Image, ImageDraw


def page_count(data: bytes) -> int:
    pdf = pdfium.PdfDocument(data)
    try:
        return len(pdf)
    finally:
        pdf.close()


def render_page(
    data: bytes, index: int, scale: float, highlight: Optional[Sequence[float]] = None
) -> Tuple[bytes, float, float, int, int]:
    """0부터 센 쪽을 PNG 로. (png, 쪽 너비 pt, 쪽 높이 pt, 그림 너비 px, 그림 높이 px)."""
    pdf = pdfium.PdfDocument(data)
    try:
        if not 0 <= index < len(pdf):
            raise IndexError(f"page {index} out of range (0..{len(pdf) - 1})")
        page = pdf[index]
        width, height = page.get_size()
        image = page.render(scale=scale).to_pil().convert("RGB")
    finally:
        pdf.close()
    if highlight:
        x0, y0, x1, y1 = (max(0.0, min(v, lim)) * scale
                          for v, lim in zip(highlight, (width, height, width, height)))
        overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
        ImageDraw.Draw(overlay).rectangle([x0, y0, x1, y1], fill=(255, 235, 51, 89),
                                          outline=(255, 191, 0, 255), width=max(1, round(1.5 * scale)))
        image = Image.alpha_composite(image.convert("RGBA"), overlay).convert("RGB")
    buf = io.BytesIO()
    image.save(buf, "PNG")
    return buf.getvalue(), width, height, image.width, image.height


def iter_images(data: bytes) -> Iterator[Tuple[int, int, bytes, str]]:
    """쪽에 놓인 그림마다 (0부터 센 쪽, 쪽 안 순번, bytes, 확장자). JPEG 은 원본 그대로, 나머지는 PNG 로."""
    pdf = pdfium.PdfDocument(data)
    try:
        for index in range(len(pdf)):
            objects = pdf[index].get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE], max_depth=15)
            for n, obj in enumerate(objects):
                try:
                    buf = io.BytesIO()
                    obj.extract(buf)
                    raw = buf.getvalue()
                except Exception as e:
                    print(f"[pdf_render] page {index + 1} image {n} 추출 실패: {e}")
                    continue
                ext = "jpg" if raw[:3] == b"\xff\xd8\xff" else "jp2" if raw[4:8] == b"jP  " else "png"
                yield index, n, raw, ext
    finally:
        pdf.close()
