"""프로젝트 루트의 ``.env``를 소스리스 배포에서도 안전하게 로드한다."""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_project_dotenv(*, override: bool = True) -> bool:
    """``find_dotenv()``의 소스 파일 스택 탐색 없이 루트 ``.env``를 로드한다.

    ``.env`` 가 OS 환경변수를 이긴다. 컨테이너에는 ``.env`` 가 없고(.dockerignore)
    k8s 가 환경변수로 주입하므로 배포는 영향이 없다. 로컬에서는 셸에 남은 옛 값이
    ``.env`` 를 덮어 엉뚱한 자격증명으로 도는 사고를 막는다.
    """
    return load_dotenv(dotenv_path=_PROJECT_ROOT / ".env", override=override)
