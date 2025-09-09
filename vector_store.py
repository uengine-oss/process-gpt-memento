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
                print(f"Processing document: {doc.metadata.get('file_name', 'unknown')}")
                
                # 이미지 분석이 이미 완료되었으므로 추가 처리 불필요
                if 'extracted_images' in doc.metadata and doc.metadata['extracted_images']:
                    print(f"Found {len(doc.metadata['extracted_images'])} extracted images (analysis already completed)")
                
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
                print(f"Document processed successfully. Content length: {len(doc.page_content)}")
            
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
            
            # 문서 저장 후 이미지 정보 저장 (이미지 분석 결과도 임베딩하여 저장)
            for doc in processed_documents:
                if 'extracted_images' in doc.metadata and doc.metadata['extracted_images']:
                    doc_id = doc.metadata.get('id')
                    print(f"Saving image metadata to database for document: {doc_id}")
                    
                    # 이미지 분석 결과가 이미 메타데이터에 포함되어 있음
                    await self._save_image_metadata(
                        doc.metadata['extracted_images'], 
                        doc_id, 
                        tenant_id,
                        doc.metadata.get('image_analysis', [])
                    )
            
            return True
        except Exception as e:
            print(f"Error adding documents to vector store: {e}")
            return False

    async def _save_image_metadata(self, images: List[Dict], document_id: str, tenant_id: str, image_analysis: List[Dict]):
        """이미지 메타데이터를 별도 테이블에 저장하고 이미지 분석 결과도 임베딩하여 저장 - URL 기반 버전"""
        try:
            # 이미지 분석 결과를 image_id로 매핑
            analysis_map = {analysis['image_id']: analysis for analysis in image_analysis}
            
            for image in images:
                try:
                    image_id = image.get('image_id', 'unknown')
                    image_name = image.get('image_name', 'unknown')
                    
                    # 이미지 분석 결과 가져오기
                    analysis_result = analysis_map.get(image_id, {})
                    analysis_text = analysis_result.get('analysis', '')

                    # 이미지 분석 텍스트가 있으면 임베딩 생성
                    image_embedding = None
                    if analysis_text:
                        try:
                            image_embedding = self.embeddings.embed_query(analysis_text)
                            print(f"Generated embedding for image {image_id}")
                        except Exception as embed_error:
                            print(f"Error generating embedding for image {image_id}: {embed_error}")
                    
                    # 이미지 메타데이터 저장 (URL 기반, base64 데이터 제거)
                    image_data = {
                        "id": str(uuid.uuid4()),
                        "document_id": document_id,
                        "tenant_id": tenant_id,
                        "image_id": image_id,
                        "image_url": image.get('image_url', ''),
                        "metadata": image.get('metadata', {})
                    }
                    
                    # 이미지 임베딩이 있으면 별도로 저장 (documents 테이블에)
                    if image_embedding:
                        # 이미지 분석 결과를 별도 문서로 저장하여 검색 가능하게 함
                        image_doc_data = {
                            "id": str(uuid.uuid4()),
                            "content": analysis_text,
                            "metadata": {
                                "type": "image_analysis",
                                "image_id": image_id,
                                "document_id": document_id,
                                "tenant_id": tenant_id,
                                "source": "image_extraction",
                                "file_name": f"{image_name}",
                                "image_url": image.get('image_url', '')
                            },
                            "embedding": image_embedding
                        }
                        
                        # 이미지 분석 결과를 documents 테이블에 저장
                        self.supabase.table("documents").insert(image_doc_data).execute()
                        print(f"Saved image analysis embedding for image: {image_id}")

                    # document_images 테이블에 이미지 메타데이터 저장
                    self.supabase.table("document_images").insert(image_data).execute()
                    print(f"Successfully saved image metadata for image: {image_id}")
                    
                except Exception as img_error:
                    print(f"Error saving metadata for individual image {image.get('image_id', 'unknown')}: {img_error}")
                    continue
        except Exception as e:
            print(f"Error in _save_image_metadata: {e}")

    async def similarity_search(self, query: str, filter: Optional[Dict[str, Any]] = None) -> List[Document]:
        """Search for similar documents asynchronously."""
        try:
            print(f"Searching for documents similar to query: {query}")
            return await asyncio.to_thread(self._similarity_search_sync, query, filter)
        except Exception as e:
            print(f"Error searching documents: {e}")
            return []

    def _similarity_search_sync(self, query: str, filter: Optional[Dict[str, Any]] = None) -> List[Document]:
        try:
            if not filter:
                filter = {}
            query_embedding = self.embeddings.embed_query(query)
            response = self.supabase.rpc(
                'match_documents',
                {
                    'query_embedding': query_embedding,
                    'filter': filter,
                    'match_count': 5
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
    
    def get_retriever(self, **kwargs):
        """Get a retriever for the vector store."""
        return self.vector_store.as_retriever(search_kwargs={"k": 5}) 