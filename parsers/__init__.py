"""PDF 파서 전략 레지스트리."""
from typing import Dict, Optional, Type

from . import config
from .base import BaseParser
from .pymupdf_parser import PyMuPDFParser
from .pdfplumber_parser import PdfplumberParser


_REGISTRY: Dict[str, Type[BaseParser]] = {
    PyMuPDFParser.name: PyMuPDFParser,
    PdfplumberParser.name: PdfplumberParser,
}


def available_strategies() -> list[str]:
    return list(_REGISTRY.keys())


def get_pdf_parser(strategy: Optional[str] = None) -> BaseParser:
    name = (strategy or config.PDF_STRATEGY or "pymupdf").strip().lower()
    cls = _REGISTRY.get(name)
    if cls is None:
        print(f"[parsers] 알 수 없는 전략 '{name}' → 'pymupdf'로 폴백")
        cls = PyMuPDFParser
        name = "pymupdf"
    print(f"[parsers] PDF 파서 '{name}' 사용")
    return cls()


def log_active_strategy() -> None:
    name = (config.PDF_STRATEGY or "pymupdf").strip().lower()
    if name not in _REGISTRY:
        name = "pymupdf"
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
    "PyMuPDFParser",
    "PdfplumberParser",
    "get_pdf_parser",
    "available_strategies",
    "log_active_strategy",
]
