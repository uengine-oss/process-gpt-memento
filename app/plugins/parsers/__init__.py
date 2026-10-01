"""파서 전략 레지스트리.

- PDF 로컬 파서: `get_pdf_parser()` (pymupdf_region 기본 / pymupdf / pdfplumber)
"""
from typing import Dict, Optional, Type

# 파서 출력이 바뀌면 올린다. knowledge_files.parser_version 으로 재인덱싱 대상을 고른다.
PARSER_VERSION = "2026-10-01.subrows"


def parser_version() -> str:
    """기록·비교에 쓰는 버전. 표 LLM 을 켜면 그 모델이 붙어, 켜고 끄거나 모델을 바꾼 파일이 옛 버전이 된다."""
    from .table_llm import enabled

    if not enabled():
        return PARSER_VERSION
    from app.core.config import resolve_llm_config

    return f"{PARSER_VERSION}+table-llm:{resolve_llm_config(role='table')['model']}"

from . import config
from .base import BaseParser
from .pymupdf_parser import PyMuPDFParser
from .pymupdf_region_parser import PyMuPDFRegionParser
from .pdfplumber_parser import PdfplumberParser


_REGISTRY: Dict[str, Type[BaseParser]] = {
    PyMuPDFParser.name: PyMuPDFParser,
    PyMuPDFRegionParser.name: PyMuPDFRegionParser,
    PdfplumberParser.name: PdfplumberParser,
}


def available_strategies() -> list[str]:
    return list(_REGISTRY.keys())


def get_pdf_parser(strategy: Optional[str] = None) -> BaseParser:
    name = (strategy or config.PDF_STRATEGY or "pymupdf_region").strip().lower()
    cls = _REGISTRY.get(name)
    if cls is None:
        print(f"[parsers] 알 수 없는 전략 '{name}' → 'pymupdf_region'로 폴백")
        cls = PyMuPDFRegionParser
        name = "pymupdf_region"
    print(f"[parsers] PDF 파서 '{name}' 사용")
    return cls()


def log_active_strategy() -> None:
    name = (config.PDF_STRATEGY or "pymupdf_region").strip().lower()
    if name not in _REGISTRY:
        name = "pymupdf_region"
    lines = [
        "",
        "=" * 60,
        " Parser configuration",
        "=" * 60,
        f"  pdf strategy : {name}",
        f"  available    : {', '.join(_REGISTRY.keys())}",
        "=" * 60,
        "",
    ]
    print("\n".join(lines), flush=True)


__all__ = [
    "BaseParser",
    "PyMuPDFParser",
    "PdfplumberParser",
    "get_pdf_parser",
    "available_strategies",
    "log_active_strategy",
]
