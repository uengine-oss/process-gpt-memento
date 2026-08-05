"""프로젝트 루트의 ``.env``를 소스리스 배포에서도 안전하게 로드한다."""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_project_dotenv(*, override: bool = False) -> bool:
    """``find_dotenv()``의 소스 파일 스택 탐색 없이 루트 ``.env``를 로드한다."""
    return load_dotenv(dotenv_path=_PROJECT_ROOT / ".env", override=override)
