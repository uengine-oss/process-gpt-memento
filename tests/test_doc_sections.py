# -*- coding: utf-8 -*-
"""카드 창이 고른 섹션 시작 → 문서 전체를 덮는 목차. 큰 섹션은 다시 나눈다."""
import asyncio

from app.services import doc_sections
from app.services.doc_cards import build_card, make_block_windows


def _blocks(sizes):
    return [{"block_index": i, "kind": "paragraph", "text": "가" * n, "heading_level": None}
            for i, n in enumerate(sizes)]


def test_windows_follow_block_boundaries_and_mark_ids():
    blocks = _blocks([5000, 5000, 5000])
    blocks[1]["heading_level"] = 1

    windows = make_block_windows(blocks, size=11000)

    assert [w.block_ids for w in windows] == [(0, 1), (2,)]
    assert "[b1] [H1] " in windows[0].text


def test_build_card_keeps_only_sections_inside_each_window():
    blocks = _blocks([5000, 5000, 5000])
    answers = iter([
        {"title": "문서", "summary": "요약", "sections": [{"start": "b0", "title": "1. 개요"},
                                                     {"start": "b2", "title": "창 밖"}]},
        {"summary": "요약", "sections": [{"start": "b2", "title": "2. 본문"}, {"start": "b9", "title": "없는 블록"}]},
    ])

    async def ask(prompt):
        return next(answers)

    card = asyncio.run(build_card(file_name="a.docx", blocks=blocks, ask=ask))

    assert [(s["start"], s["title"]) for s in card.sections] == [(0, "1. 개요"), (2, "2. 본문")]


def test_normalize_covers_document_and_drops_repeated_titles():
    blocks = _blocks([100, 100, 100, 100])
    raw = [{"start": 1, "title": "Ⅴ 향후일정"}, {"start": 2, "title": "Ⅴ 향후일정"}, {"start": 3, "title": "붙임"}]

    sections = doc_sections.normalize(raw, blocks)

    assert [(s["start"], s["end"], s["title"]) for s in sections] == [
        (0, 0, doc_sections.FRONT_TITLE), (1, 2, "Ⅴ 향후일정"), (3, 3, "붙임"),
    ]


def test_oversized_section_is_subdivided_by_llm():
    blocks = _blocks([2500] * 6)

    async def ask(prompt):
        return {"sections": [{"start": "b0", "title": "목표 1"}, {"start": "b3", "title": "목표 2"}]}

    sections = asyncio.run(doc_sections.finalize([{"start": 0, "title": "붙임 3"}], blocks, "a.hwpx", ask))

    assert [(s["start"], s["end"], s["title"]) for s in sections] == [
        (0, 2, "붙임 3 › 목표 1"), (3, 5, "붙임 3 › 목표 2"),
    ]
    assert [s["section_index"] for s in sections] == [0, 1]


def test_split_title_skips_masked_rows():
    table = "| 구분 | 내용 |\n| --- | --- |\n| ****** | ***** |\n| 사업목적 | 시민 서비스 |"
    blocks = [{"block_index": 0, "kind": "table", "text": table + "\n" + "| x | " + "가" * 7990 + " |"},
              {"block_index": 1, "kind": "table", "text": table}]

    parts = doc_sections._size_split({"start": 0, "end": 1, "title": "붙임", "summary": ""}, blocks)

    assert parts[1]["title"] == "붙임 (계속 1: 사업목적 시민 서비스)"


def test_oversized_section_falls_back_to_size_split():
    blocks = _blocks([3000] * 6)

    async def ask(prompt):
        return None

    sections = asyncio.run(doc_sections.finalize([{"start": 0, "title": "붙임 3"}], blocks, "a.hwpx", ask))

    assert all(s["chars"] <= doc_sections.SECTION_MAX_CHARS for s in sections)
    assert sections[0]["title"] == "붙임 3" and sections[1]["title"].startswith("붙임 3 (계속 1: 가")
    assert sections[-1]["end"] == 5
