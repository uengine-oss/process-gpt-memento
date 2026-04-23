"""PDF 파서 전략 레지스트리."""
from typing import Dict, Optional, Type

from . import config
from .base import BaseParser
from .pymupdf4llm_parser import PyMuPDF4LLMParser
from .pdfplumber_parser import PdfplumberParser


_REGISTRY: Dict[str, Type[BaseParser]] = {
    PyMuPDF4LLMParser.name: PyMuPDF4LLMParser,
    PdfplumberParser.name: PdfplumberParser,
}


def available_strategies() -> list[str]:
    return list(_REGISTRY.keys())


def get_pdf_parser(strategy: Optional[str] = None) -> BaseParser:
    name = (strategy or config.PDF_STRATEGY or "pymupdf4llm").strip().lower()
    cls = _REGISTRY.get(name)
    if cls is None:
        print(f"[parsers] 알 수 없는 전략 '{name}' → 'pymupdf4llm'로 폴백")
        cls = PyMuPDF4LLMParser
        name = "pymupdf4llm"
    print(f"[parsers] PDF 파서 '{name}' 사용")
    return cls()


def log_active_strategy() -> None:
    name = (config.PDF_STRATEGY or "pymupdf4llm").strip().lower()
    if name not in _REGISTRY:
        name = "pymupdf4llm"
    lines = [
        "",
        "=" * 60,
        " PDF parser configuration",
        "=" * 60,
        f"  strategy  : {name}",
        f"  available : {', '.join(_REGISTRY.keys())}",
        "=" * 60,
        "",
    ]
    print("\n".join(lines), flush=True)


__all__ = [
    "BaseParser",
    "PyMuPDF4LLMParser",
    "PdfplumberParser",
    "get_pdf_parser",
    "available_strategies",
    "log_active_strategy",
]
