"""process-gpt-document-parser 로 문서를 읽어 memento Document 로 바꾼다.

PDF 는 쪽마다 Document 하나(본문 + blocks_json 의 offset·bbox), 흐르는 문서(HWP·HWPX·DOC·DOCX)는
Document 하나에 블록 목록(BLOCKS_KEY)을 싣는다. 그림 설명·스캔 쪽 OCR 은 memento 의 VLM 을 콜백으로 넘긴다.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

from langchain.schema import Document
from process_gpt_document_parser import Document as ParsedDocument
from process_gpt_document_parser import parse as parse_document

from app.services.document_blocks import BLOCKS_KEY

from . import vision
from .base import BaseParser

# 로고·사진은 SKIP 으로 걸러 내고 정보가 있는 그림만 글로 옮긴다. 근거: 라이브러리 docs/BENCHMARKS.md "문서 속 그림"
DESCRIBE_PROMPT = """이 그림은 문서 본문 중간에 들어 있던 것이다. 문서를 읽는 LLM 이 그림의 정보를 쓸 수 있게 글로 옮겨라.
- 로고·표어·사진·장식처럼 문서 고유의 정보가 없으면 SKIP 이라고만 답한다.
- 그림이 표일 때만 마크다운 표로 옮긴다. 글은 그대로, 그래프는 제목과 "항목: 값" 목록으로(표로 만들지 않는다),
  도식은 단계·관계를 목록으로 옮긴다.
- 그림에 보이는 글자·숫자만, 그림 속 언어 그대로 쓴다. 추측해서 채우거나 번역하지 않는다."""


def _describe(data: bytes, mime: str) -> str:
    text = vision.describe_image(data, mime, prompt=DESCRIBE_PROMPT).strip()
    return "" if text.upper().startswith("SKIP") else text


def read(data: bytes, file_name: str) -> ParsedDocument:
    on = vision.pdf_vision_enabled()
    return parse_document(
        data, filename=file_name,
        describe=_describe if on else None,
        ocr=vision.ocr_page_image if on else None,
        max_workers=vision.VISION_MAX_WORKERS,
    )


def pdf_pages(parsed: ParsedDocument, file_name: str) -> List[Document]:
    """쪽별 Document. 본문은 '# 페이지 N' 머리 뒤에 블록을 빈 줄로 잇고, blocks_json 에 블록 자리를 적는다."""
    docs: List[Document] = []
    for page_num, (blocks, size) in enumerate(zip(parsed.pages(), parsed.page_sizes or [])):
        header = f"# 페이지 {page_num + 1}\n\n"
        parts: List[str] = []
        spots: List[Dict[str, Any]] = []
        cursor = len(header)
        for block in blocks:
            text = block.render()
            if not text.strip():
                continue
            if parts:
                cursor += 2
            spots.append({"offset": cursor, "length": len(text), "bbox": list(block.bbox) if block.bbox else None})
            parts.append(text)
            cursor += len(text)
        metadata: Dict[str, Any] = {
            "source": file_name,
            "page": page_num,
            "blocks_json": json.dumps(spots, ensure_ascii=False) if spots else "",
            "page_width": size[0],
            "page_height": size[1],
        }
        if any(b.ocr for b in blocks):
            metadata["vision_ocr"] = True
        described = sum(1 for b in blocks if b.kind == "image" and b.image and b.image.description)
        if described:
            metadata["vision_regions"] = described
        docs.append(Document(page_content=header + "\n\n".join(parts) if parts else "", metadata=metadata))
    return docs


def flowing_document(parsed: ParsedDocument) -> Document:
    """흐르는 문서 하나. 블록은 document_blocks 입력 형식(kind·text·heading_level)."""
    blocks: List[Dict[str, Any]] = []
    for block in parsed.blocks:
        text = block.text if block.kind == "paragraph" and block.heading_level else block.render()
        if not text.strip():
            continue
        blocks.append({"kind": block.kind, "text": text, "heading_level": block.heading_level})
    return Document(page_content=parsed.to_markdown(), metadata={BLOCKS_KEY: blocks} if blocks else {})


class DocumentParserPdf(BaseParser):
    name = "document_parser"
    supported_extensions = (".pdf",)

    async def parse(self, file_content: bytes, file_name: str) -> List[Document]:
        parsed = await asyncio.to_thread(read, file_content, file_name)
        return self._tag(pdf_pages(parsed, file_name))


async def parse_flowing(data: bytes, file_name: str) -> Optional[List[Document]]:
    parsed = await asyncio.to_thread(read, data, file_name)
    doc = flowing_document(parsed)
    doc.metadata.setdefault("parser_name", DocumentParserPdf.name)
    return [doc]
