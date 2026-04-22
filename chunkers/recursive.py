"""RecursiveCharacterTextSplitter 기반 청커 (현재 기본값)."""
import asyncio
from typing import List, Optional
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

from .base import BaseChunker


class RecursiveChunker(BaseChunker):
    name = "recursive"

    def __init__(self, chunk_size: int = 2000, chunk_overlap: int = 400, **kwargs):
        super().__init__(chunk_size=chunk_size, chunk_overlap=chunk_overlap, **kwargs)
        # 문단 → 줄바꿈 → 문장부호 → 공백 순으로 재귀 분할한다.
        self._splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            separators=["\n\n", "\n", ".", "!", "?", " ", ""],
            length_function=len,
            is_separator_regex=False,
        )

    async def split(
        self,
        documents: List[Document],
        file_name: Optional[str] = None,
    ) -> List[Document]:
        chunks = await asyncio.to_thread(self._splitter.split_documents, documents)
        return self._tag(chunks)
