"""HWPX 파일 텍스트 추출 모듈 (표 → 마크다운 변환 지원)"""

import logging
import zipfile
import xml.etree.ElementTree as ET


def _local_tag(elem) -> str:
    """네임스페이스를 제거한 태그명 반환"""
    t = elem.tag
    if '}' in t:
        return t.split('}', 1)[1]
    return t


def _collect_text(elem) -> str:
    """엘리먼트 하위의 모든 <t> 텍스트를 합쳐 반환 (표 내부 제외)"""
    parts = []
    for node in elem.iter():
        if _local_tag(node) == 'tbl':
            continue
        if _local_tag(node) == 't' and node.text:
            parts.append(node.text)
    return "".join(parts)


def _parse_table_to_markdown(tbl_elem) -> str:
    """hp:tbl 엘리먼트를 마크다운 표로 변환"""
    cells = []
    for tr in tbl_elem:
        if _local_tag(tr) != 'tr':
            continue
        for tc in tr:
            if _local_tag(tc) != 'tc':
                continue
            row = col = 0
            col_span = row_span = 1
            for cc in tc:
                tag = _local_tag(cc)
                if tag == 'cellAddr':
                    for k, v in cc.attrib.items():
                        if 'colAddr' in k:
                            col = int(v)
                        if 'rowAddr' in k:
                            row = int(v)
                elif tag == 'cellSpan':
                    for k, v in cc.attrib.items():
                        if 'colSpan' in k:
                            try:
                                col_span = int(v)
                            except ValueError:
                                pass
                        if 'rowSpan' in k:
                            try:
                                row_span = int(v)
                            except ValueError:
                                pass

            text_parts = []
            for sub in tc.iter():
                if _local_tag(sub) == 't' and sub.text:
                    text_parts.append(sub.text)
            text = " ".join("".join(text_parts).split())
            cells.append((row, col, col_span, row_span, text))

    if not cells:
        return ""

    max_row = max(r + rs for r, c, cs, rs, t in cells)
    max_col = max(c + cs for r, c, cs, rs, t in cells)

    grid = [["" for _ in range(max_col)] for _ in range(max_row)]
    for row, col, col_span, row_span, text in cells:
        grid[row][col] = text

    col_widths = [3] * max_col
    for r in range(max_row):
        for c in range(max_col):
            col_widths[c] = max(col_widths[c], len(grid[r][c]))

    lines = []
    for r in range(max_row):
        row_cells = [grid[r][c].ljust(col_widths[c]) for c in range(max_col)]
        lines.append("| " + " | ".join(row_cells) + " |")
        if r == 0:
            sep = ["-" * col_widths[c] for c in range(max_col)]
            lines.append("| " + " | ".join(sep) + " |")

    return "\n".join(lines)


def _walk_hwpx_body(root) -> list:
    """HWPX section XML을 순회하며 텍스트/표를 순서대로 추출"""
    results = []

    def _walk(elem):
        tag = _local_tag(elem)

        if tag == 'tbl':
            md = _parse_table_to_markdown(elem)
            if md:
                results.append(md)
            return

        if tag == 'p':
            has_tbl = any(
                _local_tag(ch) == 'tbl'
                for desc in elem.iter() for ch in [desc]
                if _local_tag(ch) == 'tbl' and ch is not elem
            )
            if has_tbl:
                for child in elem:
                    _walk(child)
            else:
                text = _collect_text(elem)
                if text.strip():
                    results.append(text)
            return

        for child in elem:
            _walk(child)

    _walk(root)
    return results


def extract_text_from_hwpx(hwpx_file_path: str) -> str:
    """
    HWPX 파일에서 텍스트를 추출합니다. 표는 마크다운 형식으로 변환됩니다.

    Args:
        hwpx_file_path (str): 텍스트를 추출할 HWPX 파일의 경로

    Returns:
        str: 추출된 텍스트. 오류 발생 시 빈 문자열을 반환합니다.
    """
    all_text_sections = []

    try:
        with zipfile.ZipFile(hwpx_file_path, 'r') as zipf:
            file_list = zipf.namelist()
            section_files = sorted([
                f for f in file_list
                if f.startswith('Contents/section') and f.endswith('.xml')
            ])

            if not section_files:
                raise ValueError("No 'section*.xml' files found in the HWPX archive.")

            for section_file in section_files:
                root = ET.fromstring(zipf.read(section_file))
                parts = _walk_hwpx_body(root)
                if parts:
                    all_text_sections.append("\n\n".join(parts))

    except zipfile.BadZipFile:
        logging.error(f"Error: Not a HWPX file or the file is corrupted -> {hwpx_file_path}")
        return ""
    except ET.ParseError:
        logging.error(f"Error: Failed to parse XML content in -> {hwpx_file_path}")
        return ""
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return ""

    return "\n\n".join(all_text_sections)