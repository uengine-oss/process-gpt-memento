"""
하이브리드 청커.

2-pass 전략:
  1. 먼저 Markdown/헤더 기반으로 분할해 문서 구조를 최대한 보존한다.
  2. 그래도 `chunk_size`를 초과하는 청크는 RecursiveCharacterTextSplitter로
     추가 분할한다. 이때 첫 단계에서 붙인 section_path 메타는 그대로 유지한다.

프로덕션 권장 기본값이다. 헤더가 없는 문서가 들어와도 안전하게 recursive
분할로 폴백하면서, 구조가 있는 문서에는 섹션 컨텍스트를 유지해준다.
"""
from typing import List, Optional
from langchain.schema import Document

from .base import BaseChunker
from .markdown_header import MarkdownHeaderChunker
from .recursive import RecursiveChunker


class HybridChunker(BaseChunker):
    name = "hybrid"

    def __init__(self, chunk_size: int = 2000, chunk_overlap: int = 400, **kwargs):
        super().__init__(chunk_size=chunk_size, chunk_overlap=chunk_overlap, **kwargs)
        self._md = MarkdownHeaderChunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        self._recursive = RecursiveChunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    async def split(
        self,
        documents: List[Document],
        file_name: Optional[str] = None,
    ) -> List[Document]:
        # 1단계: 구조 기반 분할
        structured = await self._md.split(documents, file_name=file_name)

        # 2단계: 크기 초과분만 추가로 recursive 분할
        final: List[Document] = []
        oversized: List[Document] = []
        for chunk in structured:
            if len(chunk.page_content or "") <= self.chunk_size:
                final.append(chunk)
            else:
                oversized.append(chunk)

        if oversized:
            refined = await self._recursive.split(oversized, file_name=file_name)
            # markdown 단계에서 부여된 section_path는 recursive가 건드리지 않으므로 그대로 유지된다.
            final.extend(refined)

        return self._tag(final)
