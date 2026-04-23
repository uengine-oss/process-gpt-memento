"""파서 전략 공통 인터페이스."""
from abc import ABC, abstractmethod
from typing import List
from langchain.schema import Document


class BaseParser(ABC):
    """
    파일을 Document 리스트로 변환한다. 일반적으로 페이지당 Document 1개.
    parser_name 메타는 `_tag`가 자동 부여한다.
    """

    name: str = "base"
    supported_extensions: tuple = ()

    @abstractmethod
    async def parse(self, file_content: bytes, file_name: str) -> List[Document]:
        """파일 bytes를 받아 Document 리스트를 반환."""

    def _tag(self, docs: List[Document]) -> List[Document]:
        for d in docs:
            if d.metadata is None:
                d.metadata = {}
            d.metadata.setdefault("parser_name", self.name)
        return docs
