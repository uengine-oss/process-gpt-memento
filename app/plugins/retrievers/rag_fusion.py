"""
RAG-Fusion retriever.

Multi-Query와 동일하게 LLM이 쿼리를 N개 확장하지만, 단순 dedupe 병합이 아니라
Reciprocal Rank Fusion (RRF) 스코어로 재순위를 매긴다:

    score(d) = Σ 1 / (rrf_k + rank_q(d))

여러 쿼리에서 공통적으로 상위로 올라오는 문서가 최종 상위로 정렬되기 때문에
multi_query보다 순위 안정성이 높고, 한 쿼리에서 운 좋게 높게 나온 문서의
영향력이 자연스럽게 줄어든다.

비용: multi_query와 동일 (LLM 1회 + 임베딩 (N+1)회).
"""
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from langchain.schema import Document

from .base import BaseRetriever
from ._llm_utils import ainvoke_text, parse_numbered_list


_PROMPT = """You are an assistant that rewrites a user's search query into {n} alternative queries for a vector database.
Produce diverse paraphrases that capture different phrasings, synonyms, or aspects of the original question.
Return ONLY a numbered list of queries, no commentary.

Original query: {query}

Alternative queries:"""


def _doc_key(d: Document) -> Tuple[str, str, str, str]:
    meta = d.metadata or {}
    return (
        str(meta.get("chunk_id") or ""),
        str(meta.get("file_id") or ""),
        str(meta.get("chunk_index") or ""),
        (d.page_content or "")[:100],
    )


class RAGFusionRetriever(BaseRetriever):
    name = "rag_fusion"

    def __init__(self, top_k: int = 5, query_count: int = 3, rrf_k: int = 60, **kwargs):
        super().__init__(top_k=top_k, **kwargs)
        self.query_count = query_count
        self.rrf_k = rrf_k

    async def _generate_queries(self, query: str) -> List[str]:
        from app.services.llm import create_llm
        llm = create_llm(temperature=0.3)
        prompt = _PROMPT.format(n=self.query_count, query=query)
        text = await ainvoke_text(llm, prompt)
        queries = parse_numbered_list(text, max_items=self.query_count)
        if not queries:
            return [query]
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
        print(f"[rag_fusion] {len(queries)}개 쿼리로 검색: {queries}")

        # 각 쿼리를 병렬로 넉넉하게(2*k) 검색해 RRF 재료 확보
        fetch_k = max(k * 2, k + 5)
        results = await asyncio.gather(*[
            vector_store.similarity_search(q, filter=filter, top_k=fetch_k) for q in queries
        ], return_exceptions=True)

        # RRF 집계
        scores: Dict[Tuple[str, str, str, str], float] = {}
        best_doc: Dict[Tuple[str, str, str, str], Document] = {}
        source_queries: Dict[Tuple[str, str, str, str], List[str]] = {}

        for q, docs in zip(queries, results):
            if isinstance(docs, Exception):
                print(f"[rag_fusion] 검색 실패 '{q}': {docs}")
                continue
            for rank, d in enumerate(docs):
                key = _doc_key(d)
                scores[key] = scores.get(key, 0.0) + 1.0 / (self.rrf_k + rank + 1)
                best_doc.setdefault(key, d)
                source_queries.setdefault(key, []).append(q)

        # 점수 내림차순 정렬 → 상위 k개 선정
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:k]

        out: List[Document] = []
        for key, score in ranked:
            d = best_doc[key]
            if d.metadata is None:
                d.metadata = {}
            d.metadata["fusion_score"] = round(score, 6)
            d.metadata.setdefault("source_query", " | ".join(source_queries.get(key, [])))
            out.append(d)

        return self._tag(out)
