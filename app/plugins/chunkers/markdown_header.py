"""
마크다운/헤더 기반 청커.

# / ## / ### / #### 헤더를 경계로 분할하고, 상위 헤더 경로를 각 청크의
`section_path` 메타에 보존한다. 섹션 본문이 `chunk_size`를 초과하면
RecursiveCharacterTextSplitter로 추가 분할해 상한을 넘지 않게 한다.

상위 로더가 마크다운 형태의 텍스트를 내주면 가장 효과적이다. 헤더가
전혀 없는 평문 문서일 경우 자동으로 recursive 분할로 폴백하므로
안전한 기본 선택지다.
"""
import asyncio
from typing import List, Optional
from langchain.schema import Document
from langchain.text_splitter import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)

from .base import BaseChunker


class MarkdownHeaderChunker(BaseChunker):
    name = "markdown_header"

    def __init__(self, chunk_size: int = 2000, chunk_overlap: int = 400, **kwargs):
        super().__init__(chunk_size=chunk_size, chunk_overlap=chunk_overlap, **kwargs)
        self._header_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=[
                ("#", "h1"),
                ("##", "h2"),
                ("###", "h3"),
                ("####", "h4"),
            ],
            strip_headers=False,
        )
        self._recursive = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            separators=["\n\n", "\n", ".", "!", "?", " ", ""],
        )

    @staticmethod
    def _build_section_path(meta: dict) -> str:
        """h1 / h2 / h3 / h4 를 슬래시로 이어 섹션 경로를 만든다."""
        parts = [meta.get(k) for k in ("h1", "h2", "h3", "h4") if meta.get(k)]
        return " / ".join(str(p) for p in parts)

    async def split(
        self,
        documents: List[Document],
        file_name: Optional[str] = None,
    ) -> List[Document]:
        def _run() -> List[Document]:
            out: List[Document] = []
            for doc in documents:
                base_meta = dict(doc.metadata or {})
                text = doc.page_content or ""

                try:
                    header_docs = self._header_splitter.split_text(text)
                except Exception:
                    header_docs = []

                # 헤더가 하나도 매칭되지 않으면 해당 문서는 recursive로 폴백
                if not header_docs:
                    sub = self._recursive.split_documents([doc])
                    out.extend(sub)
                    continue

                for hd in header_docs:
                    section_meta = {**base_meta, **(hd.metadata or {})}
                    section_meta["section_path"] = self._build_section_path(hd.metadata or {})

                    if len(hd.page_content or "") <= self.chunk_size:
                        out.append(Document(page_content=hd.page_content, metadata=section_meta))
                        continue

                    # 섹션이 너무 크면 recursive로 추가 분할 (section_path는 유지)
                    sub_chunks = self._recursive.split_text(hd.page_content or "")
                    for piece in sub_chunks:
                        out.append(Document(page_content=piece, metadata=dict(section_meta)))

            return out

        chunks = await asyncio.to_thread(_run)
        return self._tag(chunks)
