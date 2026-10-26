"""문서 파서. HWP·HWPX·DOC·DOCX·PDF 는 process-gpt-document-parser 로 읽는다.

- PDF: `get_pdf_parser()` (쪽별 Document)
- 흐르는 문서: `document_parser.parse_flowing()`
"""
from importlib.metadata import version
from typing import Optional

from .base import BaseParser
from .document_parser import DocumentParserPdf

# 파서 출력이 바뀌면 올린다. knowledge_files.parser_version 으로 재인덱싱 대상을 고른다.
PARSER_VERSION = f"document-parser-{version('process-gpt-document-parser')}"


def parser_version() -> str:
    return PARSER_VERSION


def available_strategies() -> list[str]:
    return [DocumentParserPdf.name]


def get_pdf_parser(strategy: Optional[str] = None) -> BaseParser:
    return DocumentParserPdf()


def log_active_strategy() -> None:
    lines = ["", "=" * 60, " Parser configuration", "=" * 60, f"  parser : {PARSER_VERSION}", "=" * 60, ""]
    print("\n".join(lines), flush=True)


__all__ = [
    "BaseParser",
    "PARSER_VERSION",
    "parser_version",
    "get_pdf_parser",
    "available_strategies",
    "log_active_strategy",
]
