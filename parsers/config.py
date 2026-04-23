"""
PDF 파서 설정.

사용 가능한 전략:
  "pymupdf4llm"  : PyMuPDF 기반, 페이지 스트리밍 + 마크다운 출력 (권장)
  "pdfplumber"   : pdfminer 기반, 표 인식 강함 / 메모리 사용량 큼
"""

PDF_STRATEGY: str = "pymupdf4llm"
