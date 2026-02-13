"""HWP 파일 텍스트 추출 핵심 기능"""

import logging
import os

from .password import is_hwp_file_password_protected
from .hwpx import extract_text_from_hwpx
from .hwp5 import extract_text_from_hwp5


def extract_text_from_hwp(filepath):
    """
    HWP/HWPX 파일에서 텍스트를 추출합니다.
    
    Args:
        filepath (str): HWP 또는 HWPX 파일 경로
    
    Returns:
        tuple: (추출된 텍스트, 오류 메시지) 형태의 튜플
               성공시 오류 메시지는 None
    
    Raises:
        FileNotFoundError: 파일을 찾을 수 없는 경우
        PermissionError: 파일 접근 권한이 없는 경우  
        ValueError: 지원하지 않는 파일 형식인 경우
        Exception: 기타 추출 오류
    """
    # 파일 존재 확인
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filepath}")
    
    try:
        # 암호 체크 - 암호가 걸린 파일은 제외
        if is_hwp_file_password_protected(filepath):
            logging.info(f"암호 설정된 파일로 제외: {filepath}")
            return "", "암호로 보호된 파일입니다"
        
        # 파일 확장자에 따라 적절한 추출 함수 호출
        if filepath.lower().endswith('.hwpx'):
            content = extract_text_from_hwpx(filepath)
        elif filepath.lower().endswith('.hwp'):
            content = extract_text_from_hwp5(filepath)
        else:
            raise ValueError("지원하지 않는 파일 형식입니다")
        
        # 텍스트 정리 및 결과 로깅
        if content:
            # 기본적인 텍스트 정리
            content = content.strip()
            logging.info(f"HWP 텍스트 추출 성공: {filepath} ({len(content)} 문자)")
            return content, None
        else:
            error_msg = "텍스트 추출 실패: 빈 내용"
            logging.warning(f"HWP 텍스트 추출 실패: {filepath} - {error_msg}")
            return "", error_msg
            
    except Exception as e:
        error_msg = f"텍스트 추출 중 오류: {str(e)}"
        logging.error(f"HWP 텍스트 추출 오류: {filepath} - {error_msg}")
        return "", error_msg