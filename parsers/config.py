"""
파서 설정.

PDF 로컬 전략:
  "pymupdf"     : PyMuPDF(fitz) 기반, 페이지 스트리밍 + 표 y좌표 정렬 (권장)
  "pdfplumber"  : pdfminer 기반, 표 인식 강함 / 메모리 사용량 큼

폐쇄망 파서 (Synap DocuAnalyzer):
  외부 회사 내부망에서 제공하는 DA 엔진을 호출해 파싱한다.
  .env 로는 최소한의 키만 받고, 나머지 동작 파라미터는 아래 상수로 관리한다.
    SYNAP_ENABLED : "true"/"1"/"yes" 이면 활성화
    SYNAP_API_KEY : 개인 API 키 (예: SNOCR-xxxxxxxx)
    SYNAP_URL     : DA 엔진 base URL (예: http://100.1.223.138:31701)
"""
import os


PDF_STRATEGY: str = os.getenv("PDF_STRATEGY", "pymupdf")


def _as_bool(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


# ---- Synap (env로 관리) ------------------------------------------------
SYNAP_ENABLED: bool = _as_bool(os.getenv("SYNAP_ENABLED"))
SYNAP_API_KEY: str = os.getenv("SYNAP_API_KEY", "")
SYNAP_URL: str = os.getenv("SYNAP_URL", "")

# ---- Synap 동작 파라미터 (코드 상수) -----------------------------------
SYNAP_OUTPUT_TYPE: str = "md"
SYNAP_EXTENSIONS: tuple[str, ...] = (".hwp", ".hwpx", ".pdf", ".docx", ".pptx", ".xlsx")
SYNAP_POLL_INTERVAL: float = 1.0       # 상태 폴링 간격(초)
SYNAP_POLL_TIMEOUT: float = 300.0      # 상태 폴링 최대 대기(초)
SYNAP_REQUEST_TIMEOUT: float = 60.0    # 개별 HTTP 요청 타임아웃(초)
