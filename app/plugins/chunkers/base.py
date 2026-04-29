"""모든 청킹 전략의 공통 인터페이스."""
from abc import ABC, abstractmethod
from typing import List, Optional
from langchain.schema import Document


class BaseChunker(ABC):
    """
    모든 청커는 입력 Document 리스트를 받아 분할된 Document 리스트를 반환하는
    `split()` 비동기 메서드 하나만 구현하면 된다.

    chunk_id, chunk_index, total_chunks 등의 공통 메타데이터 부여는
    DocumentProcessor.process_documents 쪽에서 일괄 처리하므로, 청커는
    분할 로직 자체에만 집중한다.

    각 청커는 결과 청크의 metadata에 반드시 `chunker_name`을 박아야 한다.
    나중에 벡터 DB에서 전략별로 결과를 필터·비교할 때 사용한다.
    """

    name: str = "base"

    def __init__(self, chunk_size: int = 2000, chunk_overlap: int = 400, **kwargs):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.extra = kwargs

    @abstractmethod
    async def split(
        self,
        documents: List[Document],
        file_name: Optional[str] = None,
    ) -> List[Document]:
        """입력 문서들을 청크로 분할한다."""

    def _tag(self, chunks: List[Document]) -> List[Document]:
        """각 청크에 chunker_name 메타를 부여한다."""
        for c in chunks:
            if c.metadata is None:
                c.metadata = {}
            c.metadata.setdefault("chunker_name", self.name)
        return chunks
