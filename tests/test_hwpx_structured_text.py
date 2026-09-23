# -*- coding: utf-8 -*-
"""HWPX <hp:t> 안의 <hp:tab/> 뒤 텍스트가 사라지지 않아야 한다.

회귀: node.text 만 읽어 "2.1<tab/>본문" 이 "2.1" 로 잘렸다(가나 RFI 본문 85% 유실).
"""
import zipfile

from app.plugins.parsers.hwpx_structured import parse

NS = 'xmlns:hp="http://www.hancom.co.kr/hwpml/2011/paragraph"'
SECTION = (
    f'<hs:sec xmlns:hs="http://www.hancom.co.kr/hwpml/2011/section" {NS}>'
    '<hp:p><hp:run><hp:t>2.1<hp:tab/>Construction is capital intensive.'
    '<hp:lineBreak/>Second line.</hp:t></hp:run></hp:p>'
    '<hp:p><hp:run><hp:tbl><hp:tr><hp:tc>'
    '<hp:cellAddr colAddr="0" rowAddr="0"/><hp:cellSpan colSpan="1" rowSpan="1"/>'
    '<hp:subList><hp:p><hp:run><hp:t>ID<hp:tab/>REQ-01</hp:t></hp:run></hp:p></hp:subList>'
    '</hp:tc></hp:tr></hp:tbl></hp:run></hp:p>'
    '</hs:sec>'
)


def test_text_after_inline_tab_is_kept(tmp_path):
    path = tmp_path / "sample.hwpx"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("Contents/section0.xml", SECTION)

    text = parse(str(path), describe=False)

    assert "2.1 Construction is capital intensive. Second line." in text
    assert "ID REQ-01" in text
