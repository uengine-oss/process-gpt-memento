"""
벤치마크 전용 Chroma 단독 벡터 스토어.

서비스 운영 코드(`vector_store.py`)는 Supabase에 원본을 저장하고 Chroma는
임베딩 인덱스로만 쓰지만, 벤치마크는 외부 의존성 없이 격리되어야 하므로
여기서는 Chroma에 컨텐츠·메타데이터·임베딩을 전부 넣는다.

retrievers 패키지가 요구하는 인터페이스는 `similarity_search(query, filter, top_k)`
하나뿐이라 그것만 충실히 구현한다.
"""
from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from chromadb import PersistentClient
from langchain.schema import Document


PRIMITIVE_METADATA_TYPES = (str, int, float, bool)


class BenchVectorStore:
    """청커 전략별로 컬렉션을 분리해서 쓰는 Chroma 전용 스토어."""

    def __init__(self, persist_dir: str, collection_name: str, embeddings):
        persist_path = Path(persist_dir).expanduser().resolve()
        persist_path.mkdir(parents=True, exist_ok=True)

        self.persist_dir = str(persist_path)
        self.collection_name = collection_name
        self.embeddings = embeddings

        self.client = PersistentClient(path=self.persist_dir)
        # 벤치마크 재실행 시 기존 컬렉션과 섞이지 않도록 미리 지우고 다시 만든다.
        try:
            self.client.delete_collection(name=collection_name)
        except Exception:
            pass
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"},
        )

    @staticmethod
    def _sanitize(metadata: Dict[str, Any]) -> Dict[str, Any]:
        clean: Dict[str, Any] = {}
        for k, v in (metadata or {}).items():
            if v is None:
                continue
            if isinstance(v, PRIMITIVE_METADATA_TYPES):
                clean[k] = v
        return clean

    def _embed_batch(self, texts: List[str], batch_size: int = 50) -> List[List[float]]:
        out: List[List[float]] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            out.extend(self.embeddings.embed_documents(batch))
            print(f"  embed {i + len(batch)}/{len(texts)}")
        return out

    def add_documents(self, documents: List[Document]) -> None:
        if not documents:
            return
        texts = [d.page_content or "" for d in documents]
        ids = [str(uuid.uuid4()) for _ in documents]
        metadatas = [self._sanitize(d.metadata or {}) for d in documents]
        embeddings = self._embed_batch(texts)
        self.collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=texts,
            metadatas=metadatas,
        )

    def _build_where(self, filter: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not filter:
            return None
        where = {k: v for k, v in filter.items() if isinstance(v, PRIMITIVE_METADATA_TYPES)}
        if not where:
            return None
        if len(where) == 1:
            return where
        return {"$and": [{k: v} for k, v in where.items()]}

    async def similarity_search(
        self,
        query: str,
        filter: Optional[Dict[str, Any]] = None,
        top_k: int = 5,
    ) -> List[Document]:
        return await asyncio.to_thread(self._similarity_search_sync, query, filter, top_k)

    def _similarity_search_sync(
        self, query: str, filter: Optional[Dict[str, Any]], top_k: int
    ) -> List[Document]:
        qvec = self.embeddings.embed_query(query)
        where = self._build_where(filter)
        response = self.collection.query(
            query_embeddings=[qvec],
            n_results=top_k,
            where=where,
        )
        docs_field = response.get("documents") or [[]]
        metas_field = response.get("metadatas") or [[]]
        texts = docs_field[0] if docs_field else []
        metas = metas_field[0] if metas_field else []
        return [
            Document(page_content=t or "", metadata=dict(m) if m else {})
            for t, m in zip(texts, metas)
        ]
