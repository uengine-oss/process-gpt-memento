"""PyMuPDF4LLM 기반 PDF 파서. 페이지 단위 스트리밍으로 메모리 피크 최소화."""
import os
import tempfile
import asyncio
from typing import List
from langchain.schema import Document

from .base import BaseParser


class PyMuPDF4LLMParser(BaseParser):
    name = "pymupdf4llm"
    supported_extensions = (".pdf",)

    async def parse(self, file_content: bytes, file_name: str) -> List[Document]:
        return await asyncio.to_thread(self._parse_sync, file_content, file_name)

    def _parse_sync(self, file_content: bytes, file_name: str) -> List[Document]:
        import pymupdf
        import pymupdf4llm

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(file_content)
            tmp_path = tmp.name

        docs: List[Document] = []
        try:
            pdf = pymupdf.open(tmp_path)
            for page_num in range(len(pdf)):
                try:
                    md = pymupdf4llm.to_markdown(pdf, pages=[page_num], show_progress=False)
                except Exception as e:
                    print(f"pymupdf4llm page {page_num} error: {e}; falling back to plain text")
                    try:
                        md = pdf[page_num].get_text()
                    except Exception:
                        md = ""
                docs.append(Document(
                    page_content=md or "",
                    metadata={"source": file_name, "page": page_num},
                ))
            pdf.close()
        finally:
            os.unlink(tmp_path)

        return self._tag(docs)
