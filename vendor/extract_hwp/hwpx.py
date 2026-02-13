"""HWPX 파일 텍스트 추출 모듈"""

import logging
import zipfile
import xml.etree.ElementTree as ET


def extract_text_from_hwpx(hwpx_file_path: str) -> str:
    """
    HWPX 파일에서 텍스트를 추출합니다.
    단락(paragraph)을 기준으로 줄 바꿈을 유지합니다.

    Args:
        hwpx_file_path (str): 텍스트를 추출할 HWPX 파일의 경로

    Returns:
        str: 추출된 텍스트. 오류 발생 시 빈 문자열을 반환합니다.
    """
        
    all_text_sections = []
    
    try:
        with zipfile.ZipFile(hwpx_file_path, 'r') as zipf:
            file_list = zipf.namelist()
            section_files = sorted([f for f in file_list if f.startswith('Contents/section') and f.endswith('.xml')])
            
            if not section_files:
                raise ValueError("No 'section*.xml' files found in the HWPX archive.")

            for section_file in section_files:
                xml_content = zipf.read(section_file)
                root = ET.fromstring(xml_content)
                
                lines_in_section = []
                # 네임스페이스를 고려하여 단락(<p>) 태그 검색
                for p_element in root.iter():
                    if not p_element.tag.endswith('}p'):
                        continue
                    
                    # 각 단락 내부의 모든 텍스트(<t>)를 찾아 합침
                    line = "".join(
                        node.text for node in p_element.iter() 
                        if node.tag.endswith('}t') and node.text
                    )
                    
                    if line.strip():
                        lines_in_section.append(line)
                
                if lines_in_section:
                    section_text = "\n".join(lines_in_section)
                    all_text_sections.append(section_text)

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