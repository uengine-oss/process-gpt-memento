# -*- coding: utf-8 -*-
"""인용 문장 → 블록 범위: 공백 무시, 블록 경계를 넘는 문장, 말줄임으로 줄인 발췌."""
import asyncio

from app.api import citations

BLOCKS = [
    {"block_index": 5, "text": "제2조 ① 평균", "page_number": 1},
    {"block_index": 6, "text": "임금 산정기간 중 그 기간과 그 기간 중에 지급된 임금", "page_number": 1},
    {"block_index": 7, "text": "은 평균임금 산정기준이 되는 기간과 임금의 총액에서 각각 뺀다.", "page_number": 1},
]


def _locate(monkeypatch, quote):
    async def file_ref(tenant_id, file_id, path):
        return "f"

    async def blocks(tenant_id, ref):
        return BLOCKS

    async def sections(tenant_id, ref):
        return [{"section_index": 0, "start_block": 5, "end_block": 7, "title": "제2조"}]

    monkeypatch.setattr(citations, "_file_ref", file_ref)
    monkeypatch.setattr(citations, "_all_blocks", blocks)
    monkeypatch.setattr(citations, "_sections", sections)
    found = asyncio.run(citations.document_locate("t", quote, file_id="f"))
    return [(m["start_block"], m["end_block"]) for m in found["matches"]]


def test_quote_across_blocks(monkeypatch):
    assert _locate(monkeypatch, "그 기간 중에 지급된 임금은") == [(6, 7)]


def test_ellipsis_joins_pieces_in_order(monkeypatch):
    assert _locate(monkeypatch, "그 기간과 그 기간 중에 지급된 임금은 ... 각각 뺀다") == [(6, 7)]
    assert _locate(monkeypatch, "각각 뺀다 … 그 기간과") == []
