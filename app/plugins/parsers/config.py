"""
파서 설정.

PDF 로컬 전략:
  "pymupdf_region" : 기본. pymupdf 에 그림 영역 병합·반복 그림 제외를 더해 VLM 호출이 적다
  "pymupdf"        : 그림(XObject)마다 VLM
  "pdfplumber"     : pdfminer 기반. VLM 경로가 없어 스캔 쪽은 빈 본문이다
근거: docs/DESIGN_NOTES.md#파서-점검
"""
import os


PDF_STRATEGY: str = os.getenv("PDF_STRATEGY", "pymupdf_region")

# 괘선 없는 표를 글 블록 배치에서 되살린다(unruled_tables). 끄려면 false.
PDF_UNRULED_TABLES: bool = os.getenv("PDF_UNRULED_TABLES", "true").strip().lower() not in ("0", "false", "no")
