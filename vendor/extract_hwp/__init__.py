"""HWP 파일 텍스트 추출 라이브러리

한글과컴퓨터의 HWP 파일 포맷(HWP 5.0, HWPX)에서 텍스트를 추출하는 Python 라이브러리입니다.
암호화된 파일 감지 및 구조화된 텍스트 추출 기능을 제공합니다.
"""

__version__ = "0.1.0"
__author__ = "extract-hwp"
__description__ = "Python library for extracting text from Korean HWP files"

# 주요 함수들을 패키지 최상위에서 사용할 수 있도록 import
from .core import extract_text_from_hwp
from .password import (
    is_hwp_file_password_protected,
    is_hwpx_password_protected,
    is_hwp5_password_protected
)
from .hwpx import extract_text_from_hwpx
from .hwp5 import extract_text_from_hwp5

__all__ = [
    "extract_text_from_hwp",
    "extract_text_from_hwpx", 
    "extract_text_from_hwp5",
    "is_hwp_file_password_protected",
    "is_hwpx_password_protected",
    "is_hwp5_password_protected"
]