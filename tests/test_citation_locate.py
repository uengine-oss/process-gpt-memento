# -*- coding: utf-8 -*-
"""인용 문장 → 블록 범위: 공백·태그 무시, 블록 경계를 넘는 문장, 말줄임, 다단 끼어듦."""
import asyncio

from app.api import citations

BLOCKS = [
    {"block_index": 5, "text": "제2조 ① 평균", "page_number": 1},
    {"block_index": 6, "text": "임금 산정기간 중 그 기간과 그 기간 중에 지급된 임금", "page_number": 1},
    {"block_index": 7, "text": "은 평균임금 산정기준이 되는 기간과 임금의 총액에서 각각 뺀다.", "page_number": 1},
]


def _matches(monkeypatch, quote):
    async def file_ref(tenant_id, file_id, path):
        return "f"

    async def blocks(tenant_id, ref):
        return BLOCKS

    async def sections(tenant_id, ref):
        return [{"section_index": 0, "start_block": 5, "end_block": 7, "title": "제2조"}]

    monkeypatch.setattr(citations, "_file_ref", file_ref)
    monkeypatch.setattr(citations, "_all_blocks", blocks)
    monkeypatch.setattr(citations, "_sections", sections)
    return asyncio.run(citations.document_locate("t", quote, file_id="f"))["matches"]


def _locate(monkeypatch, quote):
    return [(m["start_block"], m["end_block"]) for m in _matches(monkeypatch, quote)]


def test_quote_across_blocks(monkeypatch):
    assert _locate(monkeypatch, "그 기간 중에 지급된 임금은") == [(6, 7)]


def test_table_cell_breaks_and_bold_are_ignored(monkeypatch):
    BLOCKS.append({"block_index": 8, "text": "| **고위험** | 궤양성 대장염<br>•크론성 대장염 |", "page_number": 2})
    try:
        assert _locate(monkeypatch, "궤양성 대장염 •크론성 대장염") == [(8, 8)]
    finally:
        BLOCKS.pop()


def test_control_characters_from_pdf_extraction_are_ignored(monkeypatch):
    # 2015 유행성각결막염 PDF: 낱말 사이에 \x01 이 끼어 추출됐다. 느슨한 일치가 아니라 정확 일치여야 한다.
    BLOCKS.append({"block_index": 8, "text": "2015년\x01 44주(15.10.25-10.31)에\x01 28.3명", "page_number": 3})
    try:
        found = _matches(monkeypatch, "2015년 44주(15.10.25-10.31)에 28.3명")
        assert [(m["start_block"], m.get("loose")) for m in found] == [(8, None)]
    finally:
        BLOCKS.pop()


def test_punctuation_changes_do_not_matter_but_wording_does(monkeypatch):
    # 2020 녹색금융 PDF: 원문 "참여기관) BH, UNEP FI 지원" 을 모델이 "참여기관: …" 로 옮겼다.
    BLOCKS.append({"block_index": 8, "text": "참여기관) BH, UNEP FI 지원(임배용)", "page_number": 2})
    try:
        assert _locate(monkeypatch, "참여기관: BH, UNEP FI 지원") == [(8, 8)]
        assert _locate(monkeypatch, "참여기관: BH, UNEP FI 후원") == []
    finally:
        BLOCKS.pop()


def test_words_split_by_another_column_match_loosely(monkeypatch):
    # 2024 앙골라개황 PDF: 옆 단의 "경제" 가 문장 사이에 끼어 추출됐다.
    BLOCKS.append({"block_index": 8, "text": "건설 사업을 위한 1억 1천만 경제 달러 규모 차관 계약을 체결", "page_number": 2})
    try:
        found = _matches(monkeypatch, "1억 1천만 달러 규모 차관 계약")
        assert [(m["start_block"], m.get("loose")) for m in found] == [(8, True)]
        assert _locate(monkeypatch, "1천만 달러") == []  # 두 낱말은 느슨하게 찾지 않는다
    finally:
        BLOCKS.pop()


def test_loose_match_takes_the_tightest_span(monkeypatch):
    # 알기 쉬운 대장암 PDF: 첫 낱말이 앞 블록에 한 번 더 나와 범위가 불필요하게 넓어졌다.
    BLOCKS.extend([
        {"block_index": 8, "text": "분변잠혈검사(대변검사)", "page_number": 2},
        {"block_index": 9, "text": "※ 분변잠혈검사 결과 양성인 경우,", "page_number": 2},
        {"block_index": 10, "text": "감소시키는 검진 효과가 확인되었습니다.", "page_number": 2},
        {"block_index": 11, "text": "대장내시경 검사를 추가적으로 받을 수 있습니다.", "page_number": 2},
    ])
    try:
        assert _locate(monkeypatch, "분변잠혈검사 결과 양성인 경우, 대장내시경 검사를 추가적으로") == [(9, 11)]
    finally:
        del BLOCKS[-4:]


def test_ellipsis_joins_pieces_in_order(monkeypatch):
    assert _locate(monkeypatch, "그 기간과 그 기간 중에 지급된 임금은 ... 각각 뺀다") == [(6, 7)]
    assert _locate(monkeypatch, "각각 뺀다 … 그 기간과") == []
