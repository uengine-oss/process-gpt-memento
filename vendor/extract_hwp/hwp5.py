"""HWP 5.0 (OLE) 파일 텍스트 추출 모듈"""

import logging
import olefile
import zlib
import struct
from typing import IO, Generator
from io import BytesIO


# HWP 5.0 파일 상수
HWP5_SUMMARY_INFO_STREAM_NAME = "\x05HwpSummaryInformation"
HWP5_FILE_HEADER_STREAM_NAME = "FileHeader"
HWP5_BODY_TEXT_STREAM_NAME = "BodyText/Section{}"
HWP5_PARA_TEXT_TAG_ID = 67


def extract_text_from_hwp5(filepath: str) -> str:
    """
    HWP 5.0 (OLE) 파일에서 구조적 파싱을 통해 순수 텍스트를 추출합니다.

    Args:
        filepath: HWP 파일의 경로.

    Returns:
        추출된 텍스트 문자열. 파일이 유효하지 않거나 오류 발생 시 빈 문자열을 반환합니다.
    """
    # HWP5 텍스트 추출 시작
    
    if not olefile.isOleFile(filepath):
        # OLE 파일이 아니면 처리하지 않음
        logging.warning(f"OLE 파일이 아님: {filepath}")
        return ""

    ole = None
    try:
        ole = olefile.OleFileIO(filepath)
        # OLE 파일 열기 성공
        
        # 모든 스트림 목록 확인
        try:
            streams = ole.listdir()
        except:
            logging.warning(f"스트림 목록을 읽을 수 없음: {filepath}")
        
        # HWP 파일 헤더 및 요약 정보 스트림 존재 여부 확인
        if not ole.exists(HWP5_FILE_HEADER_STREAM_NAME):
            logging.warning(f"FileHeader 스트림이 없음: {filepath}")
            return ""
        
        if not ole.exists(HWP5_SUMMARY_INFO_STREAM_NAME):
            logging.warning(f"SummaryInformation 스트림이 없음: {filepath}")
            return ""
        
        # HWP 필수 스트림 확인 완료

        def _parse_text_from_stream(stream: IO[bytes]) -> Generator[str, None, None]:
            """스트림에서 텍스트 레코드를 파싱하는 내부 헬퍼 함수"""
            data = stream.read()
            offset = 0
            record_count = 0
            text_record_count = 0

            # 스트림 데이터 읽기 완료

            while offset < len(data):
                try:
                    # 레코드 헤더 (4 bytes)
                    header_value = struct.unpack_from('<I', data, offset)[0]
                except struct.error:
                    # 더 이상 읽을 데이터가 없으면 중단
                    break

                tag_id = header_value & 0x3FF       # 10 bits
                size = (header_value >> 20) & 0xFFF # 12 bits
                offset += 4
                record_count += 1

                if size == 0 or offset + size > len(data):
                    # 유효하지 않은 사이즈는 건너뜀
                    continue

                # 텍스트를 담고 있는 PARA_TEXT 레코드(tag_id: 67)인지 확인
                if tag_id == HWP5_PARA_TEXT_TAG_ID:
                    text_record_count += 1
                    # PARA_TEXT 레코드 발견
                    record_data = data[offset : offset + size]
                    text_parts = []
                    text_offset = 0

                    # 레코드 데이터 내에서 2바이트(유니코드)씩 순회하며 문자 추출
                    while text_offset < len(record_data):
                        if text_offset + 2 > len(record_data):
                            break
                        
                        char_code = struct.unpack_from('<H', record_data, text_offset)[0]
                        text_offset += 2
                        
                        # 제어 문자 처리
                        if char_code < 32:
                            if char_code in [10, 13]: # 개행 문자
                                text_parts.append('\n')
                            elif char_code == 9: # 탭 문자
                                text_parts.append('\t')
                        else:
                            # 유효한 문자 범위인지 확인 (기본 ASCII, 한글, 특수문자 등)
                            is_valid_char = (
                                0x0020 <= char_code <= 0x007E or # 기본 라틴
                                0xAC00 <= char_code <= 0xD7AF or # 한글 음절
                                0x3130 <= char_code <= 0x318F or # 한글 자모
                                0xFF00 <= char_code <= 0xFFEF or # 전각 문자
                                0x2000 <= char_code <= 0x206F  # 일반 구두점
                            )
                            if is_valid_char:
                                text_parts.append(chr(char_code))
                    
                    extracted_text = "".join(text_parts)
                    # 텍스트 추출 완료
                    yield extracted_text

                offset += size
            
            # 스트림 파싱 완료

        full_text_parts = []
        section_index = 0
        
        # BodyText/Section0, BodyText/Section1, ... 순으로 스트림 처리
        while True:
            section_stream_name = HWP5_BODY_TEXT_STREAM_NAME.format(section_index)
            if not ole.exists(section_stream_name):
                # 더 이상 섹션이 없으면 종료
                # 모든 섹션 처리 완료
                break
            
            # 섹션 처리 중

            # 스트림을 열고 압축 해제
            compressed_data = ole.openstream(section_stream_name).read()
            # 섹션 압축 데이터 처리
            
            decompressed_data = None
            try:
                # zlib 압축 해제 시도 (wbits=15)
                decompressed_data = zlib.decompress(compressed_data, 15)
                # zlib 압축 해제 성공
            except zlib.error:
                try:
                    # zlib 헤더가 없는 경우 (wbits=-15)
                    decompressed_data = zlib.decompress(compressed_data, -15)
                    # zlib 압축 해제 성공 (대체 방법)
                except zlib.error:
                    # 압축되지 않은 데이터
                    decompressed_data = compressed_data
                    # 압축되지 않은 데이터 사용
            
            # 압축 해제 완료
            
            # 압축 해제된 데이터를 메모리 스트림으로 변환
            text_stream = BytesIO(decompressed_data)

            # 스트림에서 텍스트 추출
            section_text_parts = []
            for text_part in _parse_text_from_stream(text_stream):
                section_text_parts.append(text_part)
            
            # 텍스트 블록 추출 완료
            full_text_parts.extend(section_text_parts)

            section_index += 1
            
        final_text = "".join(full_text_parts).strip()
        # HWP5 텍스트 추출 완료
        return final_text

    except (IOError, struct.error) as e:
        logging.error(f"HWP5 텍스트 추출 중 IO/구조체 오류: {filepath}, 오류: {e}")
        return ""
    except Exception as e:
        logging.error(f"HWP5 텍스트 추출 중 예상치 못한 오류: {filepath}, 오류: {e}")
        return ""
    finally:
        if ole:
            ole.close()