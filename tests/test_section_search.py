# -*- coding: utf-8 -*-
"""섹션 검색: 조사를 뗀 검색어, 키워드·벡터 순위의 RRF 결합, 한쪽 실패 시 다른 쪽으로 답함."""
import asyncio

from app.services import section_search


def test_query_terms_strip_josa_and_short_tokens():
    assert section_search.query_terms("계약금액은 얼마 A 와 위탁료를") == ["계약금액은", "계약금액", "얼마", "위탁료를", "위탁료"]


def test_rrf_prefers_sections_ranked_by_both(monkeypatch):
    async def keyword(tenant_id, file_ids, terms, limit):
        return [{"file_id": "a", "section_index": 1, "matched_terms": ["위탁료"]},
                {"file_id": "b", "section_index": 0, "matched_terms": ["위탁료"]}]

    async def vector(tenant_id, file_ids, query, limit):
        return [{"file_id": "b", "section_index": 0}, {"file_id": "c", "section_index": 2}]

    monkeypatch.setattr(section_search, "_keyword", keyword)
    monkeypatch.setattr(section_search, "_vector", vector)

    found = asyncio.run(section_search.search("t", ["a", "b", "c"], "위탁료", top_k=3))

    assert [(r["file_id"], r["section_index"]) for r in found["results"]] == [("b", 0), ("a", 1), ("c", 2)]
    assert found["results"][0]["ranks"] == {"keyword": 2, "vector": 1}


def test_vector_failure_still_returns_keyword_results(monkeypatch):
    async def keyword(tenant_id, file_ids, terms, limit):
        return [{"file_id": "a", "section_index": 1, "matched_terms": ["위탁료"]}]

    async def vector(tenant_id, file_ids, query, limit):
        raise RuntimeError("embedding down")

    monkeypatch.setattr(section_search, "_keyword", keyword)
    monkeypatch.setattr(section_search, "_vector", vector)

    found = asyncio.run(section_search.search("t", ["a"], "위탁료", top_k=3))

    assert [(r["file_id"], r["section_index"]) for r in found["results"]] == [("a", 1)]
    assert "vector" in found["errors"]
