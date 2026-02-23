from typing import List, Dict, Any, Optional
import os
import uuid
from dotenv import load_dotenv
from supabase import create_client
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores.supabase import SupabaseVectorStore
from langchain.schema import Document
import asyncio


load_dotenv()

class VectorStoreManager:
    """Manages vector store operations with Supabase"""
    
    def __init__(self):
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )
        
        self.embeddings = OpenAIEmbeddings(
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        self.vector_store = SupabaseVectorStore(
            client=self.supabase,
            embedding=self.embeddings,
            table_name="documents",
            query_name="match_documents"
        )
        
        # OpenAI API 키 설정
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_api_base = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")

    async def add_documents(self, documents: List[Document], tenant_id: str) -> bool:
        """Add documents to the vector store with image analysis and embeddings."""
        try:
            print(f"Adding {len(documents)} documents to vector store...")
            
            processed_documents = []
            
            for doc in documents:
                # 메타데이터 업데이트
                doc.metadata.update({
                    'tenant_id': tenant_id,
                    'id': str(uuid.uuid4())
                })
                
                if doc.page_content is None:
                    doc.page_content = ""
                if doc.metadata.get('source') is None:
                    doc.metadata['source'] = "Unknown"
                    
                processed_documents.append(doc)
            
            print(f"Generating embeddings for {len(processed_documents)} documents...")
            texts = [doc.page_content for doc in processed_documents]
            metadatas = [doc.metadata for doc in processed_documents]
            embeddings = self.embeddings.embed_documents(texts)
            
            print("Inserting documents into vector store...")
            for i, (text, metadata, embedding) in enumerate(zip(texts, metadatas, embeddings)):
                try:
                    self.supabase.table("documents").insert({
                        "id": metadata.get('id'),
                        "content": text,
                        "metadata": metadata,
                        "embedding": embedding
                    }).execute()
                    print(f"Successfully inserted document {i+1}/{len(processed_documents)}")
                except Exception as e:
                    print(f"Error inserting document {i+1}: {e}")
                    continue
            
            # 이미지 메타데이터: image_analysis가 있는 청크만 처리 (해당 페이지 청크에만 이미지 있음)
            await self._save_image_metadata_once(processed_documents, tenant_id)

            return True
        except Exception as e:
            print(f"Error adding documents to vector store: {e}")
            return False

    async def _save_image_metadata_once(
        self, processed_documents: List[Document], tenant_id: str
    ) -> None:
        """이미지 메타데이터를 고유 이미지당 1회만 저장 (image_analysis가 있는 청크만 처리)"""
        try:
            # 이미 저장한 image_id (임베딩 중복 방지)
            saved_embedding_ids: set = set()
            total_images_saved = 0

            for doc in processed_documents:
                image_analysis = doc.metadata.get('image_analysis') or []
                if not image_analysis:
                    continue

                doc_id = doc.metadata.get('id')
                # extracted_images에서 image_id -> 전체 이미지 정보 매핑
                extracted_map = {
                    img.get('image_id'): img
                    for img in (doc.metadata.get('extracted_images') or [])
                }

                for analysis in image_analysis:
                    image_id = analysis.get('image_id')
                    if not image_id:
                        continue
                    image_info = extracted_map.get(image_id)
                    if not image_info:
                        continue

                    analysis_text = analysis.get('analysis', '')
                    image_name = image_info.get('image_name', image_id)

                    # 임베딩: 고유 image_id당 1회만 생성
                    if analysis_text and image_id not in saved_embedding_ids:
                        try:
                            image_embedding = self.embeddings.embed_query(analysis_text)
                            saved_embedding_ids.add(image_id)
                            image_doc_data = {
                                "id": str(uuid.uuid4()),
                                "content": analysis_text,
                                "metadata": {
                                    "type": "image_analysis",
                                    "image_id": image_id,
                                    "document_id": doc_id,
                                    "tenant_id": tenant_id,
                                    "source": "image_extraction",
                                    "file_name": str(image_name),
                                    "image_url": image_info.get('image_url', ''),
                                },
                                "embedding": image_embedding,
                            }
                            self.supabase.table("documents").insert(image_doc_data).execute()
                        except Exception as e:
                            print(f"Error generating embedding for image {image_id}: {e}")

                    # document_images 테이블에 저장 (청크-이미지 매핑)
                    image_data = {
                        "id": str(uuid.uuid4()),
                        "document_id": doc_id,
                        "tenant_id": tenant_id,
                        "image_id": image_id,
                        "image_url": image_info.get('image_url', ''),
                        "metadata": image_info.get('metadata', {}),
                    }
                    self.supabase.table("document_images").insert(image_data).execute()
                    total_images_saved += 1

            if total_images_saved:
                print(f"Saved image metadata: {total_images_saved} chunk-image link(s), {len(saved_embedding_ids)} unique image embedding(s)")
        except Exception as e:
            print(f"Error in _save_image_metadata_once: {e}")

    async def similarity_search(self, query: str, filter: Optional[Dict[str, Any]] = None, top_k: int = 5) -> List[Document]:
        """Search for similar documents asynchronously."""
        try:
            print(f"Searching for documents similar to query: {query}")
            return await asyncio.to_thread(self._similarity_search_sync, query, filter, top_k)
        except Exception as e:
            print(f"Error searching documents: {e}")
            return []

    def _similarity_search_sync(self, query: str, filter: Optional[Dict[str, Any]] = None, top_k: int = 5) -> List[Document]:
        try:
            if not filter:
                filter = {}
            query_embedding = self.embeddings.embed_query(query)
            response = self.supabase.rpc(
                'match_documents',
                {
                    'query_embedding': query_embedding,
                    'filter': filter,
                    'match_count': top_k
                }
            ).execute()
            results = []
            for match in response.data:
                doc = Document(
                    page_content=match['content'],
                    metadata=match['metadata']
                )
                results.append(doc)
            print(f"Found {len(results)} similar documents")
            return results
        except Exception as e:
            print(f"Error searching documents: {e}")
            return []

    def get_retriever(self, top_k: int = 5, **kwargs):
        """Get a retriever for the vector store."""
        return self.vector_store.as_retriever(search_kwargs={"k": top_k})

    async def get_chunks_by_indices(
        self,
        tenant_id: str,
        file_name: str,
        chunk_indices: List[int],
    ) -> List[Document]:
        """Supabase documents 테이블에서 chunk_index 리스트로 청크를 직접 조회한다."""
        try:
            return await asyncio.to_thread(
                self._get_chunks_by_indices_sync, tenant_id, file_name, chunk_indices
            )
        except Exception as e:
            print(f"Error fetching chunks by indices: {e}")
            return []

    def _get_chunks_by_indices_sync(
        self,
        tenant_id: str,
        file_name: str,
        chunk_indices: List[int],
    ) -> List[Document]:
        """chunk_indices가 비어있거나 조회 실패 시 빈 리스트를 반환한다.

        Supabase PostgREST는 JSONB 경로(metadata->>) + .in_() 조합이 불안정하므로
        해당 문서의 모든 청크를 가져온 뒤 Python에서 필터링한다.
        """
        if not chunk_indices:
            return []
        target_set = {int(i) for i in chunk_indices}
        try:
            response = (
                self.supabase.table("documents")
                .select("content, metadata")
                .eq("metadata->>tenant_id", tenant_id)
                .eq("metadata->>file_name", file_name)
                .execute()
            )
            results = []
            for row in response.data or []:
                meta = row.get("metadata") or {}
                if meta.get("type") == "image_analysis":
                    continue
                try:
                    idx = int(meta.get("chunk_index", -1))
                except (TypeError, ValueError):
                    idx = -1
                if idx in target_set:
                    results.append(Document(
                        page_content=row.get("content") or "",
                        metadata=meta,
                    ))
            print(f"Fetched {len(results)} chunks by indices for {file_name}")
            return results
        except Exception as e:
            print(f"Error in _get_chunks_by_indices_sync: {e}")
            return []

    async def get_all_chunks_metadata(
        self, tenant_id: str, file_name: str
    ) -> List[Dict[str, Any]]:
        """특정 문서의 모든 청크 메타데이터(chunk_index, section_title, page_number 등)를 반환한다."""
        try:
            return await asyncio.to_thread(
                self._get_all_chunks_metadata_sync, tenant_id, file_name
            )
        except Exception as e:
            print(f"Error fetching chunks metadata: {e}")
            return []

    def _get_all_chunks_metadata_sync(
        self, tenant_id: str, file_name: str
    ) -> List[Dict[str, Any]]:
        try:
            response = (
                self.supabase.table("documents")
                .select("metadata")
                .eq("metadata->>tenant_id", tenant_id)
                .eq("metadata->>file_name", file_name)
                .order("metadata->>chunk_index")
                .execute()
            )
            results = []
            for row in response.data or []:
                meta = row["metadata"] or {}
                if meta.get("type") == "image_analysis":
                    continue
                results.append({
                    "chunk_index": meta.get("chunk_index"),
                    "section_title": meta.get("section_title") or meta.get("content", "")[:50],
                    "page_number": meta.get("page_number") or meta.get("page"),
                    "content_length": meta.get("content_length"),
                })
            return results
        except Exception as e:
            print(f"Error in _get_all_chunks_metadata_sync: {e}")
            return []