# -*- coding: utf-8 -*-
"""파일에 명시된 헤딩만 헤딩으로 본다: DOCX 제목 스타일·outlineLvl, HWPX 개요 수준."""
import zipfile

from app.plugins.parsers.docx_structured import heading_styles, parse_blocks
from app.plugins.parsers.hwpx_structured import parse_blocks as parse_hwpx_blocks

W = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
STYLES = (
    f'<w:styles {W}>'
    '<w:style w:styleId="1"><w:name w:val="heading 1"/></w:style>'
    '<w:style w:styleId="Custom"><w:name w:val="장 제목"/><w:basedOn w:val="1"/></w:style>'
    '<w:style w:styleId="Body"><w:name w:val="Normal"/></w:style>'
    '</w:styles>'
)
DOCUMENT = (
    f'<w:document {W}><w:body>'
    '<w:p><w:pPr><w:pStyle w:val="Custom"/></w:pPr><w:r><w:t>제1장 총칙</w:t></w:r></w:p>'
    '<w:p><w:pPr><w:outlineLvl w:val="1"/></w:pPr><w:r><w:t>1.1 목적</w:t></w:r></w:p>'
    '<w:p><w:pPr><w:pStyle w:val="Body"/></w:pPr><w:r><w:t>본문</w:t></w:r></w:p>'
    '<w:sdt><w:sdtContent><w:p><w:r><w:t>목차 안 문단</w:t></w:r></w:p></w:sdtContent></w:sdt>'
    '</w:body></w:document>'
)


def test_docx_heading_levels_follow_styles_and_outline():
    blocks = parse_blocks(DOCUMENT.encode(), {}, heading_styles(STYLES.encode()))

    assert [(b["text"], b["heading_level"]) for b in blocks] == [
        ("제1장 총칙", 1), ("1.1 목적", 2), ("본문", None), ("목차 안 문단", None),
    ]


HP = 'xmlns:hp="http://www.hancom.co.kr/hwpml/2011/paragraph"'
HH = 'xmlns:hh="http://www.hancom.co.kr/hwpml/2011/head"'


def test_hwpx_outline_paragraph_is_heading(tmp_path):
    path = tmp_path / "doc.hwpx"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("Contents/header.xml", (
            f'<hh:head {HH}><hh:paraProperties>'
            '<hh:paraPr id="7"><hh:heading type="OUTLINE" idRef="0" level="0"/></hh:paraPr>'
            '<hh:paraPr id="0"><hh:heading type="NONE" idRef="0" level="0"/></hh:paraPr>'
            '</hh:paraProperties></hh:head>'
        ))
        z.writestr("Contents/section0.xml", (
            f'<hs:sec xmlns:hs="http://www.hancom.co.kr/hwpml/2011/section" {HP}>'
            '<hp:p paraPrIDRef="7"><hp:run><hp:t>제1장 총칙</hp:t></hp:run></hp:p>'
            '<hp:p paraPrIDRef="0"><hp:run><hp:t>본문</hp:t></hp:run></hp:p>'
            '</hs:sec>'
        ))

    blocks = parse_hwpx_blocks(str(path), describe=False)

    assert [(b["text"], b["heading_level"]) for b in blocks] == [("제1장 총칙", 1), ("본문", None)]
