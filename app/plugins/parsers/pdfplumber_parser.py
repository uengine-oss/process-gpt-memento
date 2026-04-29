"""pdfplumber 기반 PDF 파서. 표 인식 강점, 메모리 사용량 큼."""
import os
import tempfile
import asyncio
from typing import Any, Dict, List
from langchain.schema import Document

from .base import BaseParser


class PdfplumberParser(BaseParser):
    name = "pdfplumber"
    supported_extensions = (".pdf",)

    async def parse(self, file_content: bytes, file_name: str) -> List[Document]:
        return await asyncio.to_thread(self._parse_sync, file_content, file_name)

    def _parse_sync(self, file_content: bytes, file_name: str) -> List[Document]:
        import pdfplumber

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(file_content)
            tmp_path = tmp.name

        docs: List[Document] = []
        try:
            with pdfplumber.open(tmp_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    elements: List[Dict[str, Any]] = []
                    tables = page.find_tables()
                    for table in tables:
                        try:
                            extracted = table.extract()
                            if extracted:
                                elements.append({
                                    "type": "table",
                                    "y": table.bbox[1],
                                    "content": extracted,
                                })
                        except Exception:
                            continue
                    table_bboxes = [t.bbox for t in tables]
                    elements.extend(self._text_blocks(page, table_bboxes))
                    elements.sort(key=lambda x: x["y"])

                    parts: List[str] = []
                    for elem in elements:
                        if elem["type"] == "text" and elem.get("content"):
                            parts.append(elem["content"])
                        elif elem["type"] == "table":
                            md = self._table_to_markdown(elem.get("content"))
                            if md:
                                parts.append(md)

                    docs.append(Document(
                        page_content="\n\n".join(parts) if parts else "",
                        metadata={"source": file_name, "page": page_num},
                    ))
        finally:
            os.unlink(tmp_path)

        return self._tag(docs)

    @staticmethod
    def _table_to_markdown(table: List[List[Any]]) -> str:
        if not table:
            return ""
        rows = []
        header = table[0]
        rows.append("| " + " | ".join(str(c) if c not in (None, "") else "" for c in header) + " |")
        rows.append("| " + " | ".join(["---"] * len(header)) + " |")
        for row in table[1:]:
            rows.append("| " + " | ".join(str(c) if c not in (None, "") else "" for c in row) + " |")
        return "\n".join(rows)

    @staticmethod
    def _text_blocks(page: Any, table_bboxes: List[tuple]) -> List[Dict[str, Any]]:
        def in_bbox(word: Dict, bbox: tuple) -> bool:
            x0, top, x1, bottom = bbox
            h_mid = (word["x0"] + word["x1"]) / 2
            v_mid = (word["top"] + word["bottom"]) / 2
            return x0 <= h_mid <= x1 and top <= v_mid <= bottom

        try:
            all_words = page.extract_words()
        except Exception:
            return []
        words = [w for w in all_words if not any(in_bbox(w, bb) for bb in table_bboxes)]
        if not words:
            return []

        lines: Dict[float, List[tuple]] = {}
        for w in words:
            y = round(w["top"], 1)
            lines.setdefault(y, []).append((w["x0"], w.get("text", "")))

        blocks: List[Dict[str, Any]] = []
        current: List[tuple] = []
        prev_y = None
        for y in sorted(lines.keys()):
            line = sorted(lines[y], key=lambda x: x[0])
            text = " ".join(w[1] for w in line)
            if prev_y is not None and (y - prev_y) > 20 and current:
                blocks.append({
                    "type": "text",
                    "y": current[0][0],
                    "content": "\n".join(l[1] for l in current),
                })
                current = []
            current.append((y, text))
            prev_y = y
        if current:
            blocks.append({
                "type": "text",
                "y": current[0][0],
                "content": "\n".join(l[1] for l in current),
            })
        return blocks
