"""PyMuPDF(fitz) 기반 PDF 파서. 페이지 단위 스트리밍, 표는 y좌표 기반 정렬."""
import os
import tempfile
import asyncio
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
                docs.append(Document(
                    page_content=self._page_to_markdown(page, page_num),
                    metadata={"source": file_name, "page": page_num},
                ))
            pdf.close()
        finally:
            os.unlink(tmp_path)

        return self._tag(docs)

    @staticmethod
    def _page_to_markdown(page, page_num: int) -> str:
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
        items = []

        for block in page.get_text("blocks"):
            bbox, text = block[:4], block[4]
            if any(_intersects(bbox, tb) for tb in table_bboxes):
                continue
            if text.strip():
                items.append((bbox[1], text.strip()))

        for tab in valid_tables:
            try:
                items.append((tab.bbox[1], tab.to_markdown()))
            except Exception:
                continue

        items.sort(key=lambda x: x[0])
        body = "\n\n".join(c for _, c in items)
        return f"# 페이지 {page_num + 1}\n\n{body}" if body else ""
