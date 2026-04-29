"""
HyDE (Hypothetical Document Embeddings) retriever.

LLM에게 '이 질문에 대한 가상의 답변 문서'를 짧게 써달라고 시킨 뒤,
그 가상 답변 텍스트로 임베딩 유사도 검색을 수행한다.
쿼리(짧은 질문)보다 가상 답변(문서 형태)이 실제 저장된 청크와 임베딩
공간에서 더 가까운 경우가 많아, 짧은 키워드성 질문의 검색 품질이 크게 오른다.

참고: Gao et al., "Precise Zero-Shot Dense Retrieval without Relevance Labels" (2022)

비용: LLM 1회 + 임베딩 1회 + 유사도 검색 1회.
"""
from typing import List, Optional, Dict, Any
from langchain.schema import Document

from .base import BaseRetriever
from ._llm_utils import ainvoke_text


_PROMPT = """Write a short, factual passage that would plausibly answer the question below.
Do not say "I don't know" — write what a relevant document would say, as if it were excerpted from a textbook or report.
Keep it to {length}.

Question: {query}

Passage:"""


class HyDERetriever(BaseRetriever):
    name = "hyde"

    def __init__(
        self,
        top_k: int = 5,
        doc_length: str = "1-2 sentences",
        include_original_query: bool = False,
        **kwargs,
    ):
        super().__init__(top_k=top_k, **kwargs)
        self.doc_length = doc_length
        # True이면 가상 답변과 원본 쿼리 둘 다로 검색해 병합
        self.include_original_query = include_original_query

    async def _generate_hypothetical(self, query: str) -> str:
        from app.services.llm import create_llm
        llm = create_llm(temperature=0.3)
        prompt = _PROMPT.format(query=query, length=self.doc_length)
        text = await ainvoke_text(llm, prompt)
        return text or query  # 실패 시 원본 쿼리로 폴백

    async def retrieve(
        self,
        query: str,
        vector_store,
        filter: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> List[Document]:
        k = top_k if top_k is not None else self.top_k

        hypothetical = await self._generate_hypothetical(query)
        print(f"[hyde] 가상 답변 생성 완료 (len={len(hypothetical)})")

        docs = await vector_store.similarity_search(hypothetical, filter=filter, top_k=k)

        if self.include_original_query:
            orig = await vector_store.similarity_search(query, filter=filter, top_k=k)
            seen: set = set()
            merged: List[Document] = []
            for src_q, d_list in (("hyde", docs), (query, orig)):
                for d in d_list:
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
                    d.metadata.setdefault("source_query", src_q)
                    merged.append(d)
                    if len(merged) >= k:
                        break
                if len(merged) >= k:
                    break
            docs = merged[:k]
        else:
            for d in docs:
                if d.metadata is None:
                    d.metadata = {}
                d.metadata.setdefault("source_query", "hyde")

        return self._tag(docs)
