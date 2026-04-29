"""파서 전략 레지스트리.

- PDF 로컬 파서: `get_pdf_parser()` (pymupdf / pdfplumber)
- 원격 파서(Synap DocuAnalyzer): `get_synap_parser()` / `is_synap_enabled()`
  - 외부 회사 내부망에서 제공하는 DA 엔진을 사용해야 할 때 활성화.
  - hwp/hwpx/pdf/docx/pptx/xlsx 등 지원 확장자에 대해 로컬 파서 대신 우선 시도.
"""
from typing import Dict, Optional, Type

from . import config
from .base import BaseParser
from .pymupdf_parser import PyMuPDFParser
from .pdfplumber_parser import PdfplumberParser
from .synap_parser import SynapParser, SynapParseError


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


def is_synap_enabled() -> bool:
    return bool(config.SYNAP_ENABLED and config.SYNAP_API_KEY and config.SYNAP_URL)


def synap_supports(file_extension: str) -> bool:
    if not is_synap_enabled():
        return False
    ext = (file_extension or "").lower()
    return ext in config.SYNAP_EXTENSIONS


def get_synap_parser() -> SynapParser:
    return SynapParser()


def log_active_strategy() -> None:
    name = (config.PDF_STRATEGY or "pymupdf").strip().lower()
    if name not in _REGISTRY:
        name = "pymupdf"
    synap_state = "ENABLED" if is_synap_enabled() else "disabled"
    lines = [
        "",
        "=" * 60,
        " Parser configuration",
        "=" * 60,
        f"  pdf strategy : {name}",
        f"  available    : {', '.join(_REGISTRY.keys())}",
        f"  synap remote : {synap_state}",
    ]
    if is_synap_enabled():
        lines += [
            f"    url        : {config.SYNAP_URL}",
            f"    output     : {config.SYNAP_OUTPUT_TYPE}",
            f"    extensions : {', '.join(config.SYNAP_EXTENSIONS)}",
        ]
    lines += ["=" * 60, ""]
    print("\n".join(lines), flush=True)


__all__ = [
    "BaseParser",
    "PyMuPDFParser",
    "PdfplumberParser",
    "SynapParser",
    "SynapParseError",
    "get_pdf_parser",
    "get_synap_parser",
    "is_synap_enabled",
    "synap_supports",
    "available_strategies",
    "log_active_strategy",
]
