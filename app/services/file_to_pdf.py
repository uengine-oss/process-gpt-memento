"""
Generic file -> PDF conversion helper.

Used as a fallback when a source-specific parser is unavailable.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Optional


OFFICE_EXTENSIONS = {
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
    ".odt",
    ".odp",
    ".ods",
    ".rtf",
    ".hwp",
    ".hwpx",
}

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}


class FileToPdfError(RuntimeError):
    pass


def _pick_available_pdf_path(out_dir: Path, stem: str) -> Path:
    base = "".join(ch for ch in (stem or "converted") if ch not in r'\/:*?"<>|').strip() or "converted"
    candidate = out_dir / f"{base}.pdf"
    if not candidate.exists():
        return candidate
    for _ in range(30):
        suffix = uuid.uuid4().hex[:8]
        candidate = out_dir / f"{base}_{suffix}.pdf"
        if not candidate.exists():
            return candidate
    return out_dir / f"{base}_{uuid.uuid4().hex}.pdf"


def _find_soffice() -> Optional[str]:
    found = shutil.which("soffice") or shutil.which("libreoffice")
    if found:
        return found
    return None


def convert_to_pdf(input_path: str, output_dir: str) -> str:
    src = Path(input_path)
    if not src.exists():
        raise FileToPdfError(f"입력 파일이 존재하지 않습니다: {src}")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if src.suffix.lower() == ".pdf":
        return str(src)

    ext = src.suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        return _image_to_pdf(src, out_dir)

    if ext in OFFICE_EXTENSIONS:
        return _office_to_pdf(src, out_dir, allow_unknown=False)

    return _office_to_pdf(src, out_dir, allow_unknown=True)


def _image_to_pdf(src: Path, out_dir: Path) -> str:
    try:
        from PIL import Image  # type: ignore
    except Exception as e:
        raise FileToPdfError(
            "이미지 PDF 변환을 위해 Pillow가 필요합니다. (pip install pillow)"
        ) from e

    out_path = _pick_available_pdf_path(out_dir, src.stem)
    img = Image.open(src)
    if img.mode in ("RGBA", "P", "LA"):
        img = img.convert("RGB")
    img.save(out_path, "PDF", resolution=300)
    return str(out_path)


def _office_to_pdf(src: Path, out_dir: Path, allow_unknown: bool) -> str:
    soffice = _find_soffice()
    if not soffice:
        raise FileToPdfError(
            "문서를 PDF로 변환하려면 LibreOffice(soffice)가 필요합니다."
        )

    work_dir = out_dir / f".convert_{uuid.uuid4().hex}"
    work_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        soffice,
        "--headless",
        "--nologo",
        "--nolockcheck",
        "--nodefault",
        "--nofirststartwizard",
        "--convert-to",
        "pdf",
        "--outdir",
        str(work_dir),
        str(src),
    ]

    run_env = dict(os.environ)
    run_env.pop("PYTHONHOME", None)
    run_env.pop("PYTHONPATH", None)

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=180,
            env=run_env,
        )
    except subprocess.TimeoutExpired as e:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise FileToPdfError(f"PDF 변환 타임아웃: {src.name}") from e

    if proc.returncode != 0:
        shutil.rmtree(work_dir, ignore_errors=True)
        if allow_unknown:
            raise FileToPdfError(
                f"파일 PDF 변환 실패: {src.name}\nstdout={proc.stdout[-1500:]}\nstderr={proc.stderr[-1500:]}"
            )
        raise FileToPdfError(
            f"Office PDF 변환 실패: {src.name}\nstdout={proc.stdout[-1500:]}\nstderr={proc.stderr[-1500:]}"
        )

    try:
        candidates = sorted(work_dir.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            raise FileToPdfError(f"변환 결과 PDF를 찾지 못했습니다: {src.name}")
        produced = candidates[0]
        final_out = _pick_available_pdf_path(out_dir, src.stem)
        shutil.move(str(produced), str(final_out))
        return str(final_out)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
