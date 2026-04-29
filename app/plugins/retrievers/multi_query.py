"""
Multi-Query retriever.

LLM이 원본 쿼리를 N개의 패러프레이즈(동의어·다른 표현)로 확장한 뒤,
각 쿼리로 유사도 검색을 수행해 결과를 병합·중복 제거한다.

사용자가 쿼리를 어떻게 쓰든 표현 다양성을 자동으로 보완해주기 때문에
'단어 하나만 달라져도 못 찾는' 현상을 줄여준다.

비용: LLM 1회 + 임베딩 (N+1)회 유사도 검색.
"""
import asyncio
from typing import List, Optional, Dict, Any
from langchain.schema import Document

from .base import BaseRetriever
from ._llm_utils import ainvoke_text, parse_numbered_list


_PROMPT = """You are an assistant that rewrites a user's search query into {n} alternative queries for a vector database.
Produce diverse paraphrases that capture different phrasings, synonyms, or aspects of the original question.
Return ONLY a numbered list of queries, no commentary.

Original query: {query}

Alternative queries:"""


class MultiQueryRetriever(BaseRetriever):
    name = "multi_query"

    def __init__(self, top_k: int = 5, query_count: int = 3, **kwargs):
        super().__init__(top_k=top_k, **kwargs)
        self.query_count = query_count

    async def _generate_queries(self, query: str) -> List[str]:
        from app.services.llm import create_llm
        llm = create_llm(temperature=0.3)
        prompt = _PROMPT.format(n=self.query_count, query=query)
        text = await ainvoke_text(llm, prompt)
        queries = parse_numbered_list(text, max_items=self.query_count)
        # 실패/부족 시 원본 쿼리만 가지고 진행
        if not queries:
            return [query]
        # 원본도 포함해 검색 다양성 확보
        return [query] + queries

    async def retrieve(
        self,
        query: str,
        vector_store,
        filter: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> List[Document]:
        k = top_k if top_k is not None else self.top_k

        queries = await self._generate_queries(query)
        print(f"[multi_query] {len(queries)}개 쿼리로 검색: {queries}")

        # 확장 쿼리들을 병렬로 검색
        results = await asyncio.gather(*[
            vector_store.similarity_search(q, filter=filter, top_k=k) for q in queries
        ], return_exceptions=True)

        # dedupe: chunk_id 또는 (file_id, chunk_index) 기준
        seen: set = set()
        merged: List[Document] = []
        for q, docs in zip(queries, results):
            if isinstance(docs, Exception):
                print(f"[multi_query] 검색 실패 '{q}': {docs}")
                continue
            for d in docs:
                meta = d.metadata or {}
                key = (
                    str(meta.get("chunk_id") or ""),
                    str(meta.get("file_id") or ""),
                    str(meta.get("chunk_index") or ""),
                    (d.page_content or "")[:100],
                )
                if key in seen:
                    continue
                seen.add(key)
                if d.metadata is None:
                    d.metadata = {}
                d.metadata.setdefault("source_query", q)
                merged.append(d)
                if len(merged) >= k:
                    break
            if len(merged) >= k:
                break

        return self._tag(merged[:k])
