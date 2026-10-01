# -*- coding: utf-8 -*-
"""표 LLM 파싱(MEMENTO_TABLE_LLM). 근거: docs/DESIGN_NOTES.md "PDF 표: 규칙 대 LLM"."""
from app.core import config as core_config
from app.plugins import parsers
from app.plugins.parsers import config, table_llm

TEXT = "구분\n2019\n2020\n전국\n-1.1\n△0.5\n1,234\n"


def test_accepts_a_table_whose_numbers_are_in_the_text_layer():
    md = "설명 한 줄\n| 구분 | 전국 |\n|---|---|\n| 2019 | -1.1 |\n| 2020 | △0.5 |"
    assert table_llm.accept(md, TEXT) == "| 구분 | 전국 |\n|---|---|\n| 2019 | -1.1 |\n| 2020 | △0.5 |"


def test_rejects_a_number_missing_from_the_text_layer():
    assert table_llm.accept("| 구분 | 전국 |\n|---|---|\n| 2019 | -1.2 |", TEXT) is None


def test_joined_year_month_label_is_not_an_invented_value():
    assert table_llm.accept("| 기간 | 값 |\n|---|---|\n| 2019.3 | 1,234 |", TEXT)


def test_rejects_an_answer_without_a_table():
    assert table_llm.accept("표가 아닙니다", TEXT) is None


def test_apply_replaces_only_accepted_tables():
    entries = [(0.0, "제목", [0, 0, 10, 10]), (1.0, "|a|b|\n|---|---|\n|1|2|", [0, 20, 100, 60]),
               (2.0, "|c|d|\n|---|---|\n|3|4|", [0, 70, 100, 90])]
    infos = [{"key": "t0", "bbox": [0, 20, 100, 60], "text": TEXT}, {"key": "t1", "bbox": [0, 70, 100, 90], "text": TEXT}]
    results = {"t0": "| 구분 | 전국 |\n|---|---|\n| 2019 | -1.1 |", "t1": "| x | 9.9 |\n|---|---|"}
    out, used, dropped = table_llm.apply(entries, infos, results)
    assert (used, dropped) == (1, 1)
    assert out[1][1].startswith("| 구분 | 전국 |") and out[2][1] == entries[2][1] and out[0] == entries[0]


def test_table_role_overrides_model_and_provider(monkeypatch):
    monkeypatch.setenv("MEMENTO_LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_LLM_MODEL", "main-model")
    monkeypatch.delenv("MEMENTO_TABLE_LLM_PROVIDER", raising=False)
    monkeypatch.setenv("MEMENTO_TABLE_LLM_MODEL", "table-model")
    assert core_config.resolve_llm_config()["model"] == "main-model"
    assert core_config.resolve_llm_config(role="table")["model"] == "table-model"
    monkeypatch.setenv("MEMENTO_TABLE_LLM_PROVIDER", "openrouter")
    assert core_config.resolve_llm_config(role="table")["provider"] == "openrouter"


def test_parser_version_names_the_table_model_only_when_on(monkeypatch):
    monkeypatch.setattr(config, "TABLE_LLM", False)
    assert parsers.parser_version() == parsers.PARSER_VERSION
    monkeypatch.setattr(config, "TABLE_LLM", True)
    monkeypatch.setenv("MEMENTO_LLM_PROVIDER", "openai")
    monkeypatch.delenv("MEMENTO_TABLE_LLM_PROVIDER", raising=False)
    monkeypatch.setenv("MEMENTO_TABLE_LLM_MODEL", "table-model")
    assert parsers.parser_version() == f"{parsers.PARSER_VERSION}+table-llm:table-model"
