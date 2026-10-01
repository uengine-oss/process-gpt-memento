# -*- coding: utf-8 -*-
"""괘선 없는 표 복원(unruled_tables). 근거: docs/DESIGN_NOTES.md#괘선-없는-표"""
from app.plugins.parsers.unruled_tables import line_cells, rebuild


def _line(x0, y0, text, w=40, h=10):
    return {"bbox": [x0, y0, x0 + w, y0 + h], "spans": [{"text": text}]}


def _row_block(y, texts, xs):
    return {"lines": [_line(x, y, t) for x, t in zip(xs, texts)]}


def _entry(key, block):
    xs = [l["bbox"] for l in block["lines"]]
    bbox = [min(b[0] for b in xs), min(b[1] for b in xs), max(b[2] for b in xs), max(b[3] for b in xs)]
    text = "\n".join(l["spans"][0]["text"] for l in block["lines"])
    return {"key": float(key), "text": text, "bbox": bbox, "cells": line_cells(block)}


def test_rows_side_by_side_become_a_table_with_header():
    head = _row_block(100, ["지역", "시설 수", "이용 인원"], [60, 150, 240])   # 머리는 가운데 정렬이라 칸 위치가 다르다
    rows = [_row_block(114 + 14 * i, [f"권역{i}", str(i + 3), f"{i}0,000"], [60, 180, 260]) for i in range(3)]
    entries = [{"key": 0.0, "text": "앞 문단", "bbox": [60, 60, 400, 90], "cells": None}]
    entries += [_entry(i + 1, b) for i, b in enumerate([head] + rows)]
    out = rebuild(entries)
    assert [e["text"] for e in out][0] == "앞 문단"
    table = out[1]["text"].splitlines()
    assert table[0] == "| 지역 | 시설 수 | 이용 인원 |"
    assert table[2] == "| 권역0 | 3 | 00,000 |"
    assert len(out) == 2


def test_stacked_paragraph_lines_are_not_a_row():
    block = {"lines": [_line(60, 100, "첫 줄", w=300), _line(60, 114, "둘째 줄", w=300)]}
    assert line_cells(block) is None


def test_two_rows_are_not_enough():
    rows = [_row_block(100 + 14 * i, ["a", "b"], [60, 200]) for i in range(2)]
    entries = [_entry(i, b) for i, b in enumerate(rows)]
    assert [e["text"] for e in rebuild(entries)] == ["a\nb", "a\nb"]


def test_row_with_wrapped_cell_breaks_the_table():
    rows = [_row_block(100 + 14 * i, ["a", "b"], [60, 200]) for i in range(3)]
    wrapped = {"lines": [_line(60, 142, "a"), _line(200, 142, "b1"), _line(200, 156, "b2")]}
    entries = [_entry(i, b) for i, b in enumerate(rows)] + [_entry(3, wrapped)]
    out = rebuild(entries)
    assert out[0]["text"].startswith("| a | b |")
    assert out[-1]["text"] == "a\nb1\nb2"
