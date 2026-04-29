"""
시맨틱(의미 기반) 청커.

연속한 문장들을 임베딩한 뒤 인접 문장 간 코사인 유사도가 임계값 아래로
떨어지는 지점에서 청크 경계를 넣는다. 주제가 유지되는 구간을 한 청크로
묶으므로 retrieval 품질이 올라갈 가능성이 크다.

프로젝트 임베딩 팩토리(llm.create_embeddings)를 재사용하므로 openai /
openrouter / custom provider 어느 쪽이든 바로 동작한다. 폐쇄망에서도
임베딩 엔드포인트만 살아있으면 문제없다.

주의: 문서당 임베딩 배치 호출이 한 번씩 일어나므로 ingest 속도가 느려진다.
검색 품질을 ingest 속도보다 우선시할 때 선택한다.
"""
import asyncio
import math
import re
from typing import List, Optional
from langchain.schema import Document

from .base import BaseChunker

# langchain_openai 의존성을 강제하지 않도록 llm 모듈은 사용 시점에 import.


_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9])")


def _split_sentences(text: str) -> List[str]:
    """영문용 간이 문장 분리기 (정규식 기반)."""
    text = (text or "").strip()
    if not text:
        return []
    # 경계가 흔들리지 않도록 공백을 정규화
    text = re.sub(r"[ \t]+", " ", text)
    parts = _SENTENCE_RE.split(text)
    return [p.strip() for p in parts if p and p.strip()]


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class SemanticChunker(BaseChunker):
    name = "semantic"

    def __init__(
        self,
        chunk_size: int = 2000,
        chunk_overlap: int = 400,
        similarity_threshold: float = 0.75,
        min_sentences_per_chunk: int = 2,
        **kwargs,
    ):
        super().__init__(chunk_size=chunk_size, chunk_overlap=chunk_overlap, **kwargs)
        self.similarity_threshold = similarity_threshold
        self.min_sentences_per_chunk = min_sentences_per_chunk

    def _group_sentences(
        self,
        sentences: List[str],
        embeddings: List[List[float]],
    ) -> List[str]:
        """유사도가 충분히 높은 동안 문장을 누적하다가 임계 이하로 떨어지면 끊는다."""
        if not sentences:
            return []

        chunks: List[str] = []
        current: List[str] = [sentences[0]]
        current_len = len(sentences[0])

        for i in range(1, len(sentences)):
            sim = _cosine(embeddings[i - 1], embeddings[i])
            next_len = current_len + 1 + len(sentences[i])

            # 유사도가 낮아졌고 최소 문장수를 채웠거나, 크기 상한을 넘으면 컷
            should_cut = (
                sim < self.similarity_threshold
                and len(current) >= self.min_sentences_per_chunk
            ) or next_len > self.chunk_size

            if should_cut:
                chunks.append(" ".join(current))
                current = [sentences[i]]
                current_len = len(sentences[i])
            else:
                current.append(sentences[i])
                current_len = next_len

        if current:
            chunks.append(" ".join(current))
        return chunks

    async def split(
        self,
        documents: List[Document],
        file_name: Optional[str] = None,
    ) -> List[Document]:
        from app.services.llm import create_embeddings

        embedder = create_embeddings()

        out: List[Document] = []
        for doc in documents:
            sentences = _split_sentences(doc.page_content or "")
            if not sentences:
                continue

            # 문장 수가 매우 적으면 임베딩 호출 없이 한 청크로 처리
            if len(sentences) <= self.min_sentences_per_chunk:
                out.append(Document(
                    page_content=" ".join(sentences),
                    metadata=dict(doc.metadata or {}),
                ))
                continue

            try:
                vectors = await asyncio.to_thread(embedder.embed_documents, sentences)
            except Exception as e:
                # 임베딩 실패 시: 순진한 그룹핑으로 폴백해 ingest는 계속 진행
                print(f"[SemanticChunker] 임베딩 실패 ({e}) → 순진한 그룹핑으로 폴백")
                vectors = [[0.0] for _ in sentences]

            pieces = self._group_sentences(sentences, vectors)
            for piece in pieces:
                out.append(Document(
                    page_content=piece,
                    metadata=dict(doc.metadata or {}),
                ))

        return self._tag(out)
