"""
Generic file conversion helpers (LibreOffice headless 기반).

- ``convert_to_pdf``  : office/이미지/임의 문서 → PDF (parser 미지원 시 폴백용)
- ``convert_to_docx`` : 레거시 ``.doc`` 등 → ``.docx`` (메모/표/이미지 보존 → docx 파서 재사용용)

office 변환은 LibreOffice(soffice) 가 PATH 또는 표준 설치 경로에 있어야 한다.
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


class FileConvertError(RuntimeError):
    pass


# 하위 호환: 기존 호출부(document_processor 등)가 FileToPdfError 를 import/except 한다.
FileToPdfError = FileConvertError


def _pick_available_path(out_dir: Path, stem: str, ext: str) -> Path:
    base = "".join(ch for ch in (stem or "converted") if ch not in r'\/:*?"<>|').strip() or "converted"
    if not ext.startswith("."):
        ext = "." + ext
    candidate = out_dir / f"{base}{ext}"
    if not candidate.exists():
        return candidate
    for _ in range(30):
        suffix = uuid.uuid4().hex[:8]
        candidate = out_dir / f"{base}_{suffix}{ext}"
        if not candidate.exists():
            return candidate
    return out_dir / f"{base}_{uuid.uuid4().hex}{ext}"


def _find_soffice() -> Optional[str]:
    found = shutil.which("soffice") or shutil.which("libreoffice")
    if found:
        return found
    # PATH 에 없을 때 표준 설치 경로 폴백 (Windows/Linux/Mac)
    for cand in (
        r"C:\Program Files\LibreOffice\program\soffice.exe",
        r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
        "/usr/bin/soffice",
        "/usr/bin/libreoffice",
        "/opt/libreoffice/program/soffice",
        "/Applications/LibreOffice.app/Contents/MacOS/soffice",
    ):
        if os.path.exists(cand):
            return cand
    return None


def convert_to_pdf(input_path: str, output_dir: str) -> str:
    src = Path(input_path)
    if not src.exists():
        raise FileConvertError(f"입력 파일이 존재하지 않습니다: {src}")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ext = src.suffix.lower()
    if ext == ".pdf":
        return str(src)
    if ext in IMAGE_EXTENSIONS:
        return _image_to_pdf(src, out_dir)
    # office/unknown 모두 soffice 로 시도
    return _soffice_convert(src, out_dir, convert_to="pdf", out_ext=".pdf")


def convert_to_docx(input_path: str, output_dir: str) -> str:
    """레거시 .doc 등을 .docx 로 변환(메모/표/이미지 보존). 이미 .docx 면 그대로 반환."""
    src = Path(input_path)
    if not src.exists():
        raise FileConvertError(f"입력 파일이 존재하지 않습니다: {src}")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if src.suffix.lower() == ".docx":
        return str(src)
    return _soffice_convert(src, out_dir, convert_to="docx", out_ext=".docx")


def _image_to_pdf(src: Path, out_dir: Path) -> str:
    try:
        from PIL import Image  # type: ignore
    except Exception as e:
        raise FileConvertError(
            "이미지 PDF 변환을 위해 Pillow가 필요합니다. (pip install pillow)"
        ) from e

    out_path = _pick_available_path(out_dir, src.stem, ".pdf")
    img = Image.open(src)
    if img.mode in ("RGBA", "P", "LA"):
        img = img.convert("RGB")
    img.save(out_path, "PDF", resolution=300)
    return str(out_path)


def _soffice_convert(src: Path, out_dir: Path, convert_to: str, out_ext: str) -> str:
    """LibreOffice headless 로 src 를 convert_to 포맷으로 변환 → 결과 파일 경로."""
    soffice = _find_soffice()
    if not soffice:
        raise FileConvertError(
            "문서를 변환하려면 LibreOffice(soffice)가 필요합니다."
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
        convert_to,
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
        raise FileConvertError(f"문서 변환 타임아웃: {src.name}") from e

    if proc.returncode != 0:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise FileConvertError(
            f"문서 변환 실패(→{convert_to}): {src.name}\n"
            f"stdout={proc.stdout[-1500:]}\nstderr={proc.stderr[-1500:]}"
        )

    try:
        candidates = sorted(
            work_dir.glob(f"*{out_ext}"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise FileConvertError(f"변환 결과({out_ext})를 찾지 못했습니다: {src.name}")
        final_out = _pick_available_path(out_dir, src.stem, out_ext)
        shutil.move(str(candidates[0]), str(final_out))
        return str(final_out)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
