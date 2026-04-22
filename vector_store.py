from __future__ import annotations

from typing import List, Dict, Any, Optional
import asyncio
import os
from pathlib import Path
import uuid

from chromadb import PersistentClient
from dotenv import load_dotenv
from langchain.schema import Document
from supabase import create_client

from llm import create_embeddings
import config


load_dotenv()

PRIMITIVE_METADATA_TYPES = (str, int, float, bool)


class VectorStoreManager:
    """Stores source documents in Supabase and indexes embeddings in Chroma."""

    def __init__(self):
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY"),
        )
        self.embeddings = create_embeddings()
        self.supabase_write_embedding = config.supabase_write_embedding()
        self.supabase_dummy_embedding_dimensions = (
            config.supabase_dummy_embedding_dimensions()
        )

        persist_dir = Path(config.chroma_persist_directory()).expanduser()
        if not persist_dir.is_absolute():
            persist_dir = (Path(__file__).resolve().parent / persist_dir).resolve()
        persist_dir.mkdir(parents=True, exist_ok=True)

        self.chroma_collection_name = config.chroma_collection_name().strip()
        self.chroma_client = PersistentClient(path=str(persist_dir))
        self.collection = self.chroma_client.get_or_create_collection(
            name=self.chroma_collection_name,
            metadata={"hnsw:space": "cosine"},
        )

    async def add_documents(self, documents: List[Document], tenant_id: str) -> bool:
        """Add source documents to Supabase and vector index entries to Chroma."""
        try:
            print(f"Adding {len(documents)} documents to vector store...")

            processed_documents: List[Document] = []
            for doc in documents:
                doc.metadata.update(
                    {
                        "tenant_id": tenant_id,
                        # Chunks inherit a document UUID upstream, so we always assign a
                        # row-level UUID here to keep Supabase ids unique.
                        "id": str(uuid.uuid4()),
                    }
                )

                if doc.page_content is None:
                    doc.page_content = ""
                if doc.metadata.get("source") is None:
                    doc.metadata["source"] = "Unknown"

                processed_documents.append(doc)

            print(f"Generating embeddings for {len(processed_documents)} documents...")
            texts = [doc.page_content or "" for doc in processed_documents]
            metadatas = [doc.metadata for doc in processed_documents]
            embeddings = self._embed_texts(texts)

            print("Saving documents to Supabase and Chroma...")
            for i, (text, metadata, embedding) in enumerate(
                zip(texts, metadatas, embeddings), start=1
            ):
                document_row_id = str(metadata.get("id") or uuid.uuid4())
                metadata["id"] = document_row_id
                self._insert_source_document(
                    document_row_id=document_row_id,
                    text=text,
                    metadata=metadata,
                    embedding=embedding,
                )
                self._upsert_chroma_document(
                    document_row_id=document_row_id,
                    text=text,
                    metadata=metadata,
                    embedding=embedding,
                )
                print(
                    f"Saved document {i}/{len(processed_documents)} "
                    f"to Supabase and Chroma"
                )

            await self._save_image_metadata_once(processed_documents, tenant_id)
            return True
        except Exception as e:
            print(f"Error adding documents to vector store: {e}")
            return False

    def _embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings in batches to avoid provider token limits."""
        embed_batch_size = 50
        embeddings: List[List[float]] = []
        total_batches = (len(texts) - 1) // embed_batch_size + 1 if texts else 0

        for batch_start in range(0, len(texts), embed_batch_size):
            batch_texts = texts[batch_start : batch_start + embed_batch_size]
            batch_embeddings = self.embeddings.embed_documents(batch_texts)
            embeddings.extend(batch_embeddings)
            print(
                f"Embedded batch {batch_start // embed_batch_size + 1}/{total_batches} "
                f"({len(batch_texts)} docs)"
            )
        return embeddings

    def _build_supabase_payload(
        self,
        document_row_id: str,
        text: str,
        metadata: Dict[str, Any],
        embedding: Optional[List[float]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "id": document_row_id,
            "content": text,
            "metadata": metadata,
        }
        if self.supabase_write_embedding and embedding is not None:
            payload["embedding"] = embedding
        return payload

    def _insert_source_document(
        self,
        document_row_id: str,
        text: str,
        metadata: Dict[str, Any],
        embedding: Optional[List[float]] = None,
    ) -> None:
        payload = self._build_supabase_payload(
            document_row_id=document_row_id,
            text=text,
            metadata=metadata,
            embedding=embedding,
        )
        try:
            self.supabase.table("documents").insert(payload).execute()
        except Exception as exc:
            if not self.supabase_write_embedding:
                fallback_payload = dict(payload)
                fallback_dimensions = self.supabase_dummy_embedding_dimensions
                if fallback_dimensions > 0:
                    # Some existing schemas still require a vector column even though
                    # retrieval has moved to Chroma. Retry with a zero vector solely to
                    # satisfy the legacy column shape without using it for search.
                    fallback_payload["embedding"] = [0.0] * fallback_dimensions
                    try:
                        self.supabase.table("documents").insert(fallback_payload).execute()
                        return
                    except Exception as fallback_exc:
                        raise RuntimeError(
                            "Supabase documents insert failed without embedding and with "
                            f"a dummy {fallback_dimensions}-dimensional vector. "
                            "Check the documents.embedding column constraint or override "
                            "SUPABASE_DUMMY_EMBEDDING_DIMENSIONS."
                        ) from fallback_exc

                raise RuntimeError(
                    "Supabase documents insert failed without embedding. "
                    "The documents.embedding column may still require a non-null vector. "
                    "Relax or disable that constraint before using Chroma-only indexing."
                ) from exc
            raise

    def _build_chroma_metadata(
        self, metadata: Dict[str, Any], document_row_id: str
    ) -> Dict[str, Any]:
        chroma_metadata: Dict[str, Any] = {}
        for key, value in (metadata or {}).items():
            if value is None:
                continue
            if isinstance(value, PRIMITIVE_METADATA_TYPES):
                chroma_metadata[key] = value

        chroma_metadata["document_row_id"] = document_row_id
        chroma_metadata.setdefault("type", "document")
        return chroma_metadata

    def _upsert_chroma_document(
        self,
        document_row_id: str,
        text: str,
        metadata: Dict[str, Any],
        embedding: List[float],
    ) -> None:
        self.collection.upsert(
            ids=[document_row_id],
            embeddings=[embedding],
            documents=[text],
            metadatas=[self._build_chroma_metadata(metadata, document_row_id)],
        )

    async def _save_image_metadata_once(
        self, processed_documents: List[Document], tenant_id: str
    ) -> None:
        """이미지 메타데이터를 고유 이미지당 1회만 저장 (image_analysis가 있는 청크만 처리)."""
        try:
            saved_embedding_ids: set[str] = set()
            total_images_saved = 0

            for doc in processed_documents:
                image_analysis = doc.metadata.get("image_analysis") or []
                if not image_analysis:
                    continue

                doc_id = doc.metadata.get("id")
                extracted_map = {
                    img.get("image_id"): img
                    for img in (doc.metadata.get("extracted_images") or [])
                }

                for analysis in image_analysis:
                    image_id = analysis.get("image_id")
                    if not image_id:
                        continue
                    image_info = extracted_map.get(image_id)
                    if not image_info:
                        continue

                    analysis_text = analysis.get("analysis", "")
                    image_name = image_info.get("image_name", image_id)

                    if analysis_text and image_id not in saved_embedding_ids:
                        try:
                            image_embedding = self.embeddings.embed_query(analysis_text)
                            saved_embedding_ids.add(image_id)

                            image_document_row_id = str(uuid.uuid4())
                            image_metadata = {
                                "type": "image_analysis",
                                "image_id": image_id,
                                "document_id": doc_id,
                                "tenant_id": tenant_id,
                                "source": "image_extraction",
                                "file_name": str(image_name),
                                "source_file_name": doc.metadata.get("file_name", ""),
                                "drive_folder_name": doc.metadata.get(
                                    "drive_folder_name", ""
                                ),
                                "drive_folder_id": (doc.metadata or {}).get(
                                    "drive_folder_id", ""
                                ),
                                "image_url": image_info.get("image_url", ""),
                            }
                            self._insert_source_document(
                                document_row_id=image_document_row_id,
                                text=analysis_text,
                                metadata=image_metadata,
                                embedding=image_embedding,
                            )
                            self._upsert_chroma_document(
                                document_row_id=image_document_row_id,
                                text=analysis_text,
                                metadata=image_metadata,
                                embedding=image_embedding,
                            )
                        except Exception as e:
                            print(f"Error generating embedding for image {image_id}: {e}")

                    image_data = {
                        "id": str(uuid.uuid4()),
                        "document_id": doc_id,
                        "tenant_id": tenant_id,
                        "image_id": image_id,
                        "image_url": image_info.get("image_url", ""),
                        "metadata": image_info.get("metadata", {}),
                    }
                    self.supabase.table("document_images").insert(image_data).execute()
                    total_images_saved += 1

            if total_images_saved:
                print(
                    "Saved image metadata: "
                    f"{total_images_saved} chunk-image link(s), "
                    f"{len(saved_embedding_ids)} unique image embedding(s)"
                )
        except Exception as e:
            print(f"Error in _save_image_metadata_once: {e}")

    async def similarity_search(
        self,
        query: str,
        filter: Optional[Dict[str, Any]] = None,
        top_k: int = 5,
    ) -> List[Document]:
        """Search Chroma and hydrate the matching source documents from Supabase."""
        try:
            print(f"Searching for documents similar to query: {query}")
            return await asyncio.to_thread(self._similarity_search_sync, query, filter, top_k)
        except Exception as e:
            print(f"Error searching documents: {e}")
            return []

    def _build_chroma_where(
        self, filter: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if not filter:
            return None

        where: Dict[str, Any] = {}
        for key, value in filter.items():
            if value is None:
                continue
            if isinstance(value, PRIMITIVE_METADATA_TYPES):
                where[key] = value
        if not where:
            return None
        if len(where) == 1:
            return where
        return {"$and": [{key: value} for key, value in where.items()]}

    def _similarity_search_sync(
        self,
        query: str,
        filter: Optional[Dict[str, Any]] = None,
        top_k: int = 5,
    ) -> List[Document]:
        try:
            query_embedding = self.embeddings.embed_query(query)
            where = self._build_chroma_where(filter)
            response = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=top_k,
                where=where,
            )

            hit_ids = response.get("ids", [[]])
            ordered_document_ids: List[str] = []
            for raw_id in hit_ids[0] if hit_ids else []:
                document_id = str(raw_id)
                if document_id and document_id not in ordered_document_ids:
                    ordered_document_ids.append(document_id)

            results = self._fetch_documents_by_ids(ordered_document_ids)
            print(f"Found {len(results)} similar documents")
            return results
        except Exception as e:
            print(f"Error searching documents: {e}")
            return []

    def _fetch_documents_by_ids(self, document_ids: List[str]) -> List[Document]:
        if not document_ids:
            return []

        response = (
            self.supabase.table("documents")
            .select("id, content, metadata")
            .in_("id", document_ids)
            .execute()
        )

        rows_by_id = {
            str(row.get("id")): row
            for row in (response.data or [])
            if row.get("id") is not None
        }

        ordered_documents: List[Document] = []
        for document_id in document_ids:
            row = rows_by_id.get(str(document_id))
            if not row:
                continue
            ordered_documents.append(
                Document(
                    page_content=row.get("content") or "",
                    metadata=row.get("metadata") or {},
                )
            )
        return ordered_documents

    def get_retriever(self, top_k: int = 5, **kwargs):
        raise NotImplementedError(
            "SupabaseVectorStore retriever was removed. "
            "Use similarity_search() and build the RAG context explicitly."
        )

    async def get_chunks_by_indices(
        self,
        tenant_id: str,
        file_name: str,
        chunk_indices: List[int],
        drive_folder_id: Optional[str] = None,
    ) -> List[Document]:
        """Supabase documents 테이블에서 chunk_index 리스트로 청크를 직접 조회한다."""
        try:
            return await asyncio.to_thread(
                self._get_chunks_by_indices_sync,
                tenant_id,
                file_name,
                chunk_indices,
                drive_folder_id,
            )
        except Exception as e:
            print(f"Error fetching chunks by indices: {e}")
            return []

    def _get_chunks_by_indices_sync(
        self,
        tenant_id: str,
        file_name: str,
        chunk_indices: List[int],
        drive_folder_id: Optional[str] = None,
    ) -> List[Document]:
        """chunk_indices가 비어있거나 조회 실패 시 빈 리스트를 반환한다.

        Supabase PostgREST는 JSONB 경로(metadata->>) + .in_() 조합이 불안정하므로
        해당 문서의 모든 청크를 가져온 뒤 Python에서 필터링한다.
        """
        if not chunk_indices:
            return []
        target_set = {int(i) for i in chunk_indices}
        try:
            query = (
                self.supabase.table("documents")
                .select("content, metadata")
                .eq("metadata->>tenant_id", tenant_id)
                .eq("metadata->>file_name", file_name)
            )
            if drive_folder_id:
                query = query.eq("metadata->>drive_folder_id", drive_folder_id)
            response = query.execute()
            results = []
            for row in response.data or []:
                meta = row.get("metadata") or {}
                if drive_folder_id and meta.get("drive_folder_id") != drive_folder_id:
                    continue
                if meta.get("type") == "image_analysis":
                    continue
                try:
                    idx = int(meta.get("chunk_index", -1))
                except (TypeError, ValueError):
                    idx = -1
                if idx in target_set:
                    results.append(
                        Document(
                            page_content=row.get("content") or "",
                            metadata=meta,
                        )
                    )
            print(f"Fetched {len(results)} chunks by indices for {file_name}")
            return results
        except Exception as e:
            print(f"Error in _get_chunks_by_indices_sync: {e}")
            return []

    async def get_all_chunks_metadata(
        self, tenant_id: str, file_name: str, drive_folder_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """특정 문서의 모든 청크 메타데이터(chunk_index, section_title, page_number 등)를 반환한다."""
        try:
            return await asyncio.to_thread(
                self._get_all_chunks_metadata_sync, tenant_id, file_name, drive_folder_id
            )
        except Exception as e:
            print(f"Error fetching chunks metadata: {e}")
            return []

    def _get_all_chunks_metadata_sync(
        self, tenant_id: str, file_name: str, drive_folder_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        try:
            query = (
                self.supabase.table("documents")
                .select("metadata")
                .eq("metadata->>tenant_id", tenant_id)
                .eq("metadata->>file_name", file_name)
            )
            if drive_folder_id:
                query = query.eq("metadata->>drive_folder_id", drive_folder_id)
            response = query.order("metadata->>chunk_index").execute()
            results = []
            for row in response.data or []:
                meta = row["metadata"] or {}
                if drive_folder_id and meta.get("drive_folder_id") != drive_folder_id:
                    continue
                if meta.get("type") == "image_analysis":
                    continue
                results.append(
                    {
                        "chunk_index": meta.get("chunk_index"),
                        "section_title": meta.get("section_title")
                        or meta.get("content", "")[:50],
                        "page_number": meta.get("page_number") or meta.get("page"),
                        "content_length": meta.get("content_length"),
                    }
                )
            return results
        except Exception as e:
            print(f"Error in _get_all_chunks_metadata_sync: {e}")
            return []


_vector_store_instance: Optional["VectorStoreManager"] = None


def get_vector_store() -> "VectorStoreManager":
    global _vector_store_instance
    if _vector_store_instance is None:
        _vector_store_instance = VectorStoreManager()
    return _vector_store_instance