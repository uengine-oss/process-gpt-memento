"""PyMuPDF(fitz) 기반 PDF 파서. 페이지 단위 스트리밍, 표는 y좌표 기반 정렬."""
import os
import tempfile
import asyncio
import json
from typing import List
from langchain.schema import Document

from .base import BaseParser


def _intersects(a, b) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


class PyMuPDFParser(BaseParser):
    name = "pymupdf"
    supported_extensions = (".pdf",)

    async def parse(self, file_content: bytes, file_name: str) -> List[Document]:
        return await asyncio.to_thread(self._parse_sync, file_content, file_name)

    def _parse_sync(self, file_content: bytes, file_name: str) -> List[Document]:
        import pymupdf

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(file_content)
            tmp_path = tmp.name

        docs: List[Document] = []
        try:
            pdf = pymupdf.open(tmp_path)
            for page_num, page in enumerate(pdf):
                markdown, blocks, page_size = self._page_to_markdown(page, page_num)
                docs.append(Document(
                    page_content=markdown,
                    metadata={
                        "source": file_name,
                        "page": page_num,
                        # 블록별 (offset, length, bbox) 리스트 — 청킹 후 offset 매칭에 사용.
                        # Chroma는 flat primitive만 허용하므로 JSON 문자열로 저장.
                        "blocks_json": json.dumps(blocks, ensure_ascii=False) if blocks else "",
                        "page_width": page_size[0],
                        "page_height": page_size[1],
                    },
                ))
            pdf.close()
        finally:
            os.unlink(tmp_path)

        return self._tag(docs)

    @staticmethod
    def _page_to_markdown(page, page_num: int):
        """페이지를 markdown으로 변환 + 각 블록의 (offset, length, bbox)를 수집.

        Returns:
            (markdown_text, blocks_list, (page_width, page_height))
            blocks_list: [{"offset": int, "length": int, "bbox": [x0,y0,x1,y1]}, ...]
        """
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
        # items: [(y_sort_key, text, bbox)]
        items = []

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

        items.sort(key=lambda x: x[0])

        # markdown 빌드 + offset 추적
        header = f"# 페이지 {page_num + 1}\n\n"
        parts = [header]
        cursor = len(header)
        blocks: list = []
        for idx, (_y, text, bbox) in enumerate(items):
            if idx > 0:
                separator = "\n\n"
                parts.append(separator)
                cursor += len(separator)
            blocks.append({"offset": cursor, "length": len(text), "bbox": bbox})
            parts.append(text)
            cursor += len(text)

        markdown = "".join(parts) if items else ""
        # 페이지 크기 (렌더링 시 좌표 정규화용)
        rect = page.rect
        page_size = (float(rect.width), float(rect.height))
        return markdown, blocks, page_size
