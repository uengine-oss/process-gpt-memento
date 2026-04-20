"""HWP 5.0 (OLE) 파일 텍스트 추출 모듈 (표 → 마크다운 변환 지원)"""

import logging
import olefile
import zlib
import struct
from typing import List, Tuple


# HWP 5.0 파일 상수
HWP5_SUMMARY_INFO_STREAM_NAME = "\x05HwpSummaryInformation"
HWP5_FILE_HEADER_STREAM_NAME = "FileHeader"
HWP5_BODY_TEXT_STREAM_NAME = "BodyText/Section{}"

# HWP5 레코드 태그 ID (HWPTAG_BEGIN = 66)
TAG_PARA_TEXT = 67      # HWPTAG_PARA_TEXT
TAG_LIST_HEADER = 72    # HWPTAG_LIST_HEADER (표 셀 포함)
TAG_TABLE = 77          # HWPTAG_TABLE


def _parse_records(data: bytes) -> List[Tuple[int, int, bytes]]:
    """섹션 스트림의 모든 레코드를 (tag_id, level, data) 리스트로 파싱"""
    records = []
    offset = 0
    while offset + 4 <= len(data):
        header_value = struct.unpack_from('<I', data, offset)[0]
        tag_id = header_value & 0x3FF
        level = (header_value >> 10) & 0x3FF
        size = (header_value >> 20) & 0xFFF
        offset += 4
        if size == 0xFFF:
            if offset + 4 > len(data):
                break
            size = struct.unpack_from('<I', data, offset)[0]
            offset += 4
        if size == 0 or offset + size > len(data):
            offset += size
            continue
        records.append((tag_id, level, data[offset:offset + size]))
        offset += size
    return records


def _extract_para_text(record_data: bytes) -> str:
    """PARA_TEXT 레코드에서 텍스트 추출"""
    parts = []
    text_offset = 0
    while text_offset + 2 <= len(record_data):
        char_code = struct.unpack_from('<H', record_data, text_offset)[0]
        text_offset += 2
        if char_code < 32:
            if char_code in (10, 13):
                parts.append('\n')
            elif char_code == 9:
                parts.append('\t')
        else:
            is_valid = (
                0x0020 <= char_code <= 0x007E or
                0xAC00 <= char_code <= 0xD7AF or
                0x3130 <= char_code <= 0x318F or
                0xFF00 <= char_code <= 0xFFEF or
                0x2000 <= char_code <= 0x206F
            )
            if is_valid:
                parts.append(chr(char_code))
    return "".join(parts)


def _build_table_markdown(records: list, start_idx: int) -> Tuple[str, int]:
    """TABLE 레코드부터 시작해 마크다운 표를 생성하고 소비한 인덱스를 반환"""
    _, table_level, table_data = records[start_idx]

    if len(table_data) >= 8:
        n_rows = struct.unpack_from('<H', table_data, 4)[0]
        n_cols = struct.unpack_from('<H', table_data, 6)[0]
    else:
        return "", start_idx + 1

    if n_rows == 0 or n_cols == 0:
        return "", start_idx + 1

    cells = {}
    current_cell = None
    cell_texts = []
    cell_seq = 0

    i = start_idx + 1
    while i < len(records):
        tag_id, level, data = records[i]
        if level < table_level:
            break

        if tag_id == TAG_LIST_HEADER and level == table_level:
            if current_cell is not None:
                cells[current_cell] = " ".join(cell_texts).strip()
            cell_texts = []
            if len(data) >= 16:
                col_addr = struct.unpack_from('<H', data, 8)[0]
                row_addr = struct.unpack_from('<H', data, 10)[0]
                current_cell = (row_addr, col_addr)
            else:
                current_cell = (cell_seq // n_cols, cell_seq % n_cols)
            cell_seq += 1

        elif tag_id == TAG_PARA_TEXT and current_cell is not None:
            text = _extract_para_text(data).strip()
            if text:
                cell_texts.append(text)

        i += 1

    if current_cell is not None:
        cells[current_cell] = " ".join(cell_texts).strip()

    if not cells:
        return "", i

    actual_rows = max(max(r for r, c in cells) + 1, n_rows)
    actual_cols = max(max(c for r, c in cells) + 1, n_cols)
    grid = [["" for _ in range(actual_cols)] for _ in range(actual_rows)]
    for (r, c), text in cells.items():
        if 0 <= r < actual_rows and 0 <= c < actual_cols:
            grid[r][c] = text

    col_widths = [3] * actual_cols
    for r in range(actual_rows):
        for c in range(actual_cols):
            col_widths[c] = max(col_widths[c], len(grid[r][c]))

    lines = []
    for r in range(actual_rows):
        row_cells = [grid[r][c].ljust(col_widths[c]) for c in range(actual_cols)]
        lines.append("| " + " | ".join(row_cells) + " |")
        if r == 0:
            sep = ["-" * col_widths[c] for c in range(actual_cols)]
            lines.append("| " + " | ".join(sep) + " |")

    return "\n".join(lines), i


def _process_section(data: bytes) -> str:
    """섹션 데이터를 파싱하여 텍스트+마크다운 표 문자열 반환"""
    records = _parse_records(data)
    blocks = []  # (type, content)
    i = 0
    table_end_idx = -1

    while i < len(records):
        tag_id, level, rec_data = records[i]

        if tag_id == TAG_TABLE:
            md_table, next_i = _build_table_markdown(records, i)
            if md_table:
                blocks.append(('table', md_table))
            table_end_idx = next_i
            i = next_i
            continue

        if tag_id == TAG_PARA_TEXT and i >= table_end_idx:
            text = _extract_para_text(rec_data).strip()
            if text:
                blocks.append(('text', text))

        i += 1

    parts = []
    for idx, (btype, content) in enumerate(blocks):
        if idx == 0:
            parts.append(content)
            continue
        prev_type = blocks[idx - 1][0]
        if prev_type != btype or btype == 'table':
            parts.append('\n\n')
        else:
            parts.append('\n')
        parts.append(content)

    return "".join(parts)


def extract_text_from_hwp5(filepath: str) -> str:
    """
    HWP 5.0 (OLE) 파일에서 텍스트를 추출합니다. 표는 마크다운 형식으로 변환됩니다.

    Args:
        filepath: HWP 파일의 경로.

    Returns:
        추출된 텍스트 문자열. 파일이 유효하지 않거나 오류 발생 시 빈 문자열을 반환합니다.
    """
    if not olefile.isOleFile(filepath):
        logging.warning(f"OLE 파일이 아님: {filepath}")
        return ""

    ole = None
    try:
        ole = olefile.OleFileIO(filepath)

        if not ole.exists(HWP5_FILE_HEADER_STREAM_NAME):
            logging.warning(f"FileHeader 스트림이 없음: {filepath}")
            return ""

        if not ole.exists(HWP5_SUMMARY_INFO_STREAM_NAME):
            logging.warning(f"SummaryInformation 스트림이 없음: {filepath}")
            return ""

        sections = []
        section_index = 0

        while True:
            stream_name = HWP5_BODY_TEXT_STREAM_NAME.format(section_index)
            if not ole.exists(stream_name):
                break

            compressed_data = ole.openstream(stream_name).read()

            try:
                decompressed = zlib.decompress(compressed_data, 15)
            except zlib.error:
                try:
                    decompressed = zlib.decompress(compressed_data, -15)
                except zlib.error:
                    decompressed = compressed_data

            section_text = _process_section(decompressed)
            if section_text.strip():
                sections.append(section_text)

            section_index += 1

        final_text = "\n\n".join(sections).strip()
        logging.info(f"HWP5 텍스트 추출 성공: {filepath} ({len(final_text)} 문자)")
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