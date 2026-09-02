"""PDF vision 파서 임시 검증 스크립트 (수동 실행용).

사용:
  .venv/Scripts/python.exe scripts/_test_pdf_vision.py <pdf_path>
"""
import asyncio
import os
import sys
from pathlib import Path

# .env 로드 (있으면)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.plugins.parsers.pymupdf_parser import PyMuPDFParser  # noqa: E402
from app.plugins.parsers import vision  # noqa: E402
from app.core.config import resolve_llm_config  # noqa: E402


async def main(pdf_path: str):
    cfg = resolve_llm_config()
    print(f"[cfg] provider={cfg['provider']} model={cfg['model']} base_url={cfg['base_url']}")
    print(f"[cfg] pdf_vision_enabled={vision.pdf_vision_enabled()}")

    data = Path(pdf_path).read_bytes()
    parser = PyMuPDFParser()
    docs = await parser.parse(data, Path(pdf_path).name)
    print(f"\n=== {len(docs)} page-documents ===")
    for d in docs:
        flags = {k: v for k, v in d.metadata.items() if k in ("vision_ocr", "vision_figures")}
        print(f"\n----- page {d.metadata.get('page')} (len={len(d.page_content)}) {flags} -----")
        print(d.page_content)


if __name__ == "__main__":
    pdf = sys.argv[1] if len(sys.argv) > 1 else ""
    if not pdf:
        print("usage: python scripts/_test_pdf_vision.py <pdf_path>")
        sys.exit(1)
    asyncio.run(main(pdf))
