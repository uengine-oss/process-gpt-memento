"""
Query Rewrite retriever.

LLM이 원본 쿼리를 '검색 친화적' 형태로 다시 쓴 뒤 그 쿼리로 한 번 검색한다.
구어체·장문·잡음 많은 질문을 압축된 키워드 문장으로 바꿔준다.

예:
  원본: "음... 어제 본 자료에 나오던 그 A프로젝트 예산 관련된 얘기가 정확히 뭐였더라"
  재작성: "A project budget details"

비용: LLM 1회 + 임베딩 1회.
"""
from typing import List, Optional, Dict, Any
from langchain.schema import Document

from .base import BaseRetriever
from ._llm_utils import ainvoke_text


_PROMPT = """Rewrite the following user query into a clean, keyword-focused search query suitable for a vector database.
Strip filler words, preserve the key entities, topics, and constraints.
Return ONLY the rewritten query as a single line, no quotes, no commentary.

User query: {query}

Rewritten query:"""


class RewriteRetriever(BaseRetriever):
    name = "rewrite"

    async def _rewrite(self, query: str) -> str:
        from llm import create_llm
        llm = create_llm(temperature=0.0)
        prompt = _PROMPT.format(query=query)
        text = await ainvoke_text(llm, prompt)
        # 한 줄만 남기고 잡음 제거
        text = (text or "").splitlines()[0].strip() if text else ""
        return text or query

    async def retrieve(
        self,
        query: str,
        vector_store,
        filter: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> List[Document]:
        k = top_k if top_k is not None else self.top_k

        rewritten = await self._rewrite(query)
        print(f"[rewrite] '{query}' → '{rewritten}'")

        docs = await vector_store.similarity_search(rewritten, filter=filter, top_k=k)
        for d in docs:
            if d.metadata is None:
                d.metadata = {}
            d.metadata.setdefault("source_query", rewritten)
            d.metadata["original_query"] = query
        return self._tag(docs)
