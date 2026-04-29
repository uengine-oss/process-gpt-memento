"""
고정 토큰 크기 청커.

오로지 토큰 수 기준으로만 자른다 (선택적으로 overlap 지정). 구조·의미 인식
없이 기계적으로 분할하기 때문에 다른 전략들과 성능을 비교할 때 baseline으로
유용하다.

기본적으로 tiktoken `cl100k_base` 인코더를 사용한다. 폐쇄망에 tiktoken이
반입 안 돼 있으면 공백 기반 단어 분할로 근사 폴백한다.
"""
import asyncio
from typing import List, Optional
from langchain.schema import Document

from .base import BaseChunker


def _get_tiktoken_encoder():
    try:
        import tiktoken
        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


class FixedTokenChunker(BaseChunker):
    name = "fixed_token"

    # 기본값: ~500 토큰 (영문 기준 대략 2000자), overlap 100 토큰
    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 100, **kwargs):
        super().__init__(chunk_size=chunk_size, chunk_overlap=chunk_overlap, **kwargs)
        self._encoder = _get_tiktoken_encoder()

    def _split_text(self, text: str) -> List[str]:
        if not text:
            return []

        if self._encoder is not None:
            tokens = self._encoder.encode(text)
            step = max(1, self.chunk_size - self.chunk_overlap)
            pieces = []
            for start in range(0, len(tokens), step):
                window = tokens[start:start + self.chunk_size]
                if not window:
                    break
                pieces.append(self._encoder.decode(window))
                if start + self.chunk_size >= len(tokens):
                    break
            return pieces

        # tiktoken 없을 때: 공백 기반 단어 분할로 근사 폴백
        words = text.split()
        step = max(1, self.chunk_size - self.chunk_overlap)
        pieces = []
        for start in range(0, len(words), step):
            window = words[start:start + self.chunk_size]
            if not window:
                break
            pieces.append(" ".join(window))
            if start + self.chunk_size >= len(words):
                break
        return pieces

    async def split(
        self,
        documents: List[Document],
        file_name: Optional[str] = None,
    ) -> List[Document]:
        def _run() -> List[Document]:
            out: List[Document] = []
            for doc in documents:
                pieces = self._split_text(doc.page_content or "")
                for piece in pieces:
                    out.append(Document(page_content=piece, metadata=dict(doc.metadata or {})))
            return out

        chunks = await asyncio.to_thread(_run)
        return self._tag(chunks)
