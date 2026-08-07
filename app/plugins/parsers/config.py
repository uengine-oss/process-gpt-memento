"""
파서 설정.

PDF 로컬 전략:
  "pymupdf"     : PyMuPDF(fitz) 기반, 페이지 스트리밍 + 표 y좌표 정렬 (권장)
  "pdfplumber"  : pdfminer 기반, 표 인식 강함 / 메모리 사용량 큼
"""
import os


PDF_STRATEGY: str = os.getenv("PDF_STRATEGY", "pymupdf")
