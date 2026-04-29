"""
Plain retriever (기본값).

쿼리를 그대로 임베딩해 vector_store에서 top-k 유사도 검색만 수행한다.
현재 동작과 동일하며 LLM 호출이 전혀 없어 가장 빠르다.
"""
from typing import List, Optional, Dict, Any
from langchain.schema import Document

from .base import BaseRetriever


class PlainRetriever(BaseRetriever):
    name = "plain"

    async def retrieve(
        self,
        query: str,
        vector_store,
        filter: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> List[Document]:
        k = top_k if top_k is not None else self.top_k
        docs = await vector_store.similarity_search(query, filter=filter, top_k=k)
        for d in docs:
            if d.metadata is None:
                d.metadata = {}
            d.metadata.setdefault("source_query", query)
        return self._tag(docs)
