# -*- coding: utf-8 -*-
"""선 없는 하위 행 나누기(subrows). 근거: docs/DESIGN_NOTES.md#선-없는-하위-행"""
import fitz

from app.plugins.parsers.subrows import split_subrows

COLS = [50, 130, 200, 270, 340]


def _page(header, body, body_h, cols=COLS):
    """머리 한 행 + 가로선 없는 본문 한 행인 표. body = [(y, [칸 글자...])]."""
    doc = fitz.open()
    page = doc.new_page()
    top, mid, bottom = 50, 80, 80 + body_h
    for y in (top, mid, bottom):
        page.draw_line((cols[0], y), (cols[-1], y))
    for x in cols:
        page.draw_line((x, top), (x, bottom))
    for y, texts in header + body:
        for x, t in zip(cols, texts):
            if t:
                page.insert_text((x + 4, y), t, fontsize=9)
    return page


def _split(page):
    tab = page.find_tables().tables[0]
    return [ln for ln in split_subrows(page, tab, tab.to_markdown()).splitlines() if not ln.startswith("|---")]


def test_unlined_subrows_split_and_carry_year():
    body = [(94, ["2019", "1/4", "1.5", "-2.0"]), (108, ["", "2/4", "0.3", "4.1"]),
            (122, ["2020", "1/4", "-0.7", "2.2"])]
    rows = _split(_page([(70, ["year", "q", "A", "B"])], body, 50))
    assert rows[1:] == ["|2019|1/4|1.5|-2.0|", "|2019|2/4|0.3|4.1|", "|2020|1/4|-0.7|2.2|"]


def test_wrapped_header_cell_is_not_split():
    header = [(64, ["", "", "final", "diff"]), (76, ["", "", "(A)", "(A-B)"])]
    page = _page(header, [(94, ["x", "y", "1.0", "2.0"])], 20)
    tab = page.find_tables().tables[0]
    assert split_subrows(page, tab, tab.to_markdown()) == tab.to_markdown()


def test_row_holding_a_paragraph_is_not_split():
    body = [(94, ["A page border drawn as a table holds this long paragraph line", "", "", ""]),
            (108, ["", "q1", "1.0", "2.0"]), (122, ["", "q2", "3.0", "4.0"])]
    page = _page([(70, ["kind", "rate", "A", "B"])], body, 50, cols=[50, 380, 440, 500, 560])
    tab = page.find_tables().tables[0]
    assert split_subrows(page, tab, tab.to_markdown()) == tab.to_markdown()


def test_wrapped_text_cell_beside_stacked_values_is_not_split():
    body = [(94, ["1. approve", "", "9,939", "9,939"]), (106, ["2. amend", "", "(12.1)", "(12.1)"]),
            (118, ["3. elect", "", "", ""]), (130, ["4. audit", "", "", ""]), (142, ["5. pay", "", "", ""])]
    page = _page([(70, ["agenda", "n", "own", "used"])], body, 72)
    tab = page.find_tables().tables[0]
    assert split_subrows(page, tab, tab.to_markdown()) == tab.to_markdown()


def test_share_line_in_parentheses_joins_the_value_above():
    body = [(94, ["Seoul", "3", "220", "31"]), (106, ["", "", "(31.3)", "(4.3)"]),
            (118, ["", "4", "290", "54"]), (130, ["", "", "(41.8)", "(7.7)"])]
    rows = _split(_page([(70, ["area", "age", "kinder", "public"])], body, 60))
    assert rows[1:] == ["|Seoul|3|220 (31.3)|31 (4.3)|", "|Seoul|4|290 (41.8)|54 (7.7)|"]


def test_label_between_two_value_lines_goes_to_both():
    body = [(94, ["", "mom", "1.0", "2.0"]), (101, ["store", "", "", ""]), (108, ["", "yoy", "3.0", "4.0"])]
    rows = _split(_page([(70, ["kind", "rate", "A", "B"])], body, 34))
    assert rows[1:] == ["|store|mom|1.0|2.0|", "|store|yoy|3.0|4.0|"]
