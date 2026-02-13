"""HWP 파일 암호화 검증 모듈"""

import logging
import zipfile
import xml.etree.ElementTree as ET
import olefile
import struct


# HWP 5.0 파일 상수
HWP5_FILE_HEADER_STREAM_NAME = "FileHeader"


def is_hwpx_password_protected(filepath):
    """HWPX 파일이 암호로 보호되어 있는지 확인"""
    try:
        with zipfile.ZipFile(filepath, 'r') as zip_file:
            # META-INF/manifest.xml 파일 확인
            manifest_path = 'META-INF/manifest.xml'
            if manifest_path in zip_file.namelist():
                with zip_file.open(manifest_path) as manifest_file:
                    manifest_content = manifest_file.read()
                    # XML 파싱
                    root = ET.fromstring(manifest_content)
                    # odf:encryption-data 엘리먼트 검색
                    # 네임스페이스 고려하여 검색
                    for elem in root.iter():
                        if 'encryption-data' in elem.tag:
                            return True  # 암호화된 파일
                    return False  # 암호화되지 않은 파일
            else:
                logging.warning(f"HWPX 파일에 manifest.xml이 없음: {filepath}")
                return False
    except Exception as e:
        logging.warning(f"HWPX 암호파일 확인 실패 {filepath}: {e}")
        # 확인 실패 시 안전하게 False 반환 (인덱싱 진행)
        return False


def is_hwp5_password_protected(filepath):
    """
    HWP 5.0 파일의 암호화 여부를 확인합니다.

    Args:
        filepath: HWP 파일의 경로.

    Returns:
        암호화된 경우 True, 그렇지 않은 경우 False를 반환합니다.
    """
    if not olefile.isOleFile(filepath):
        return False

    ole = None
    try:
        ole = olefile.OleFileIO(filepath)
        if not ole.exists(HWP5_FILE_HEADER_STREAM_NAME):
            return False

        # FileHeader 스트림 읽기
        header_stream = ole.openstream(HWP5_FILE_HEADER_STREAM_NAME)
        header_data = header_stream.read(256) # 헤더는 256바이트

        if len(header_data) < 40: # 최소 헤더 길이 확인 (시그니처 + 버전 + 속성)
            return False

        # 속성 필드는 36번째 바이트부터 4바이트 크기의 DWORD.
        # little-endian unsigned int로 읽음
        properties = struct.unpack_from('<I', header_data, 36)[0]

        # 속성의 bit 1이 암호 설정 여부를 나타냄 (0b10)
        is_encrypted = (properties & 0x02) != 0
        return is_encrypted

    except (IOError, struct.error):
        # 파일 읽기 오류나 구조체 파싱 오류 발생 시
        return False
    finally:
        if ole:
            ole.close()


def is_hwp_file_password_protected(filepath):
    """HWP/HWPX 파일이 암호로 보호되어 있는지 확인 (통합 함수)"""
    if filepath.lower().endswith('.hwpx'):
        return is_hwpx_password_protected(filepath)
    elif filepath.lower().endswith('.hwp'):
        return is_hwp5_password_protected(filepath)
    else:
        return False