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

# 가로선 없이 한 행에 묶인 하위 행을 글자 줄로 나눈다(subrows). 끄려면 false.
PDF_SPLIT_SUBROWS: bool = os.getenv("PDF_SPLIT_SUBROWS", "true").strip().lower() not in ("0", "false", "no")

# PDF 표 영역을 LLM(잘라낸 그림 + 그 영역 PDF 글자)으로 다시 읽는다. 표마다 10초 안팎이라 기본 끔.
# 모델은 MEMENTO_TABLE_LLM_PROVIDER / _MODEL(없으면 기본 LLM). 근거: DESIGN_NOTES "PDF 표: 규칙 대 LLM".
TABLE_LLM: bool = os.getenv("MEMENTO_TABLE_LLM", "false").strip().lower() in ("1", "true", "yes", "on")
