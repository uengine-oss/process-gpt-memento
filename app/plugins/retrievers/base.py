"""모든 Retrieval 전략의 공통 인터페이스."""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from langchain.schema import Document


class BaseRetriever(ABC):
    """
    모든 retriever는 쿼리와 vector_store(유사도 검색 제공자)를 받아
    관련 Document 리스트를 돌려주는 `retrieve()` 비동기 메서드 하나만
    구현하면 된다.

    각 retriever는 결과 Document의 metadata에 반드시 `retriever_name`을
    박아야 한다. 확장 쿼리가 관여했다면 `source_query`도, 재순위 점수가
    있다면 `fusion_score`도 넣는다.
    """

    name: str = "base"

    def __init__(self, top_k: int = 5, **kwargs):
        self.top_k = top_k
        self.extra = kwargs

    @abstractmethod
    async def retrieve(
        self,
        query: str,
        vector_store,
        filter: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> List[Document]:
        """주어진 쿼리에 대해 관련 문서를 검색한다."""

    def _tag(self, docs: List[Document]) -> List[Document]:
        """각 결과에 retriever_name 메타를 부여한다."""
        for d in docs:
            if d.metadata is None:
                d.metadata = {}
            d.metadata.setdefault("retriever_name", self.name)
        return docs
