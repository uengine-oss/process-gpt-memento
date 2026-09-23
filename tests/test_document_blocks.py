# -*- coding: utf-8 -*-
"""블록은 흐르는 문서에 쪽을 달지 않고, 쪽 있는 문서에만 단다. 큰 블록은 경계에서 나눈다."""
import json

from langchain.schema import Document

from app.services.document_blocks import BLOCKS_KEY, MAX_BLOCK_CHARS, build_blocks


def test_flow_document_blocks_have_no_page():
    doc = Document(page_content="ignored", metadata={BLOCKS_KEY: [
        {"kind": "paragraph", "text": "1. 개요", "heading_level": 1},
        {"kind": "paragraph", "text": "본문", "heading_level": None},
        {"kind": "table", "text": "| a | b |\n| --- | --- |\n| 1 | 2 |", "heading_level": None},
    ]})

    blocks = build_blocks([doc])

    assert [(b["block_index"], b["kind"], b["heading_level"], b["page_number"]) for b in blocks] == [
        (0, "paragraph", 1, None), (1, "paragraph", None, None), (2, "table", None, None),
    ]


def test_pdf_page_segments_carry_page_and_bbox():
    content = "# 페이지 3\n\n첫 문단\n\n둘째 문단"
    blocks_json = json.dumps([
        {"offset": len("# 페이지 3\n\n"), "length": 4, "bbox": [1, 2, 3, 4]},
        {"offset": len("# 페이지 3\n\n첫 문단\n\n"), "length": 5, "bbox": [5, 6, 7, 8]},
    ])
    doc = Document(page_content=content, metadata={"page": 2, "blocks_json": blocks_json})

    blocks = build_blocks([doc])

    assert [(b["text"], b["page_number"], b["bbox"]) for b in blocks] == [
        ("첫 문단", 3, [1, 2, 3, 4]), ("둘째 문단", 3, [5, 6, 7, 8]),
    ]


def test_large_table_splits_on_rows_and_repeats_header():
    header = "| id | 내용 |\n| --- | --- |"
    rows = [f"| {i} | {'가' * 90} |" for i in range(60)]
    doc = Document(page_content="x", metadata={BLOCKS_KEY: [
        {"kind": "table", "text": "\n".join([header, *rows]), "heading_level": None},
    ]})

    blocks = build_blocks([doc])

    assert len(blocks) > 1
    assert all(b["text"].startswith(header) for b in blocks)
    assert all(len(b["text"]) <= MAX_BLOCK_CHARS for b in blocks)
    assert sum(b["text"].count("\n| ") - 1 for b in blocks) == len(rows)


def test_single_long_line_splits_on_spaces():
    line = " ".join(["문장"] * 1500)
    doc = Document(page_content=line, metadata={"page": 0})

    blocks = build_blocks([doc])

    assert len(blocks) > 1
    assert all(len(b["text"]) <= MAX_BLOCK_CHARS for b in blocks)
    assert " ".join(b["text"] for b in blocks) == line


def test_sheet_without_blank_lines_splits_by_size():
    sheet = "[시트: 2025]\n" + "\n".join(f"사업{i}\t{'값' * 50}" for i in range(200))
    doc = Document(page_content=sheet, metadata={"page": 0, "sheet_name": "2025"})

    blocks = build_blocks([doc])

    assert len(blocks) > 1
    assert {b["page_number"] for b in blocks} == {1}
    assert "".join(b["text"].replace("\n", "") for b in blocks) == sheet.replace("\n", "")
