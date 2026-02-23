import os
import re
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.chains.retrieval_qa.base import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain.schema import Document
from vector_store import VectorStoreManager
import asyncio

class RAGChain:
    def __init__(self):
        print("Initializing RAG Chain...")
        # Load environment variables from .env file only
        load_dotenv(override=True)
        
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY not found in .env file")
        
        # Initialize ChatOpenAI with debugging
        print("Initializing ChatOpenAI...")
        self.llm = ChatOpenAI(
            model_name="gpt-4o",  # Vision API를 지원하는 모델로 변경
            temperature=0,
            api_key=openai_api_key
        )
        print("ChatOpenAI initialized successfully")
        
        self.vector_store = VectorStoreManager()
        if self.vector_store.supabase is None:
            from supabase import create_client
            self.supabase = create_client(
                os.getenv('SUPABASE_URL'),
                os.getenv('SUPABASE_KEY')
            )
        else:
            self.supabase = self.vector_store.supabase
        
        # Create custom prompt templates for different languages
        self.prompts = {
            'ko': """다음의 맥락을 사용하여 질문에 답변해주세요. 
            답을 모른다면, 모른다고 말씀해주세요. 답을 만들어내려고 하지 마세요.
            
            맥락: {context}
            
            질문: {question}
            
            답변: """,
            
            'en': """Use the following pieces of context to answer the question at the end. 
            If you don't know the answer, just say that you don't know. Don't try to make up an answer.
            
            Context: {context}
            
            Question: {question}
            
            Answer: """
        }

    def detect_language(self, text: str) -> str:
        """Detect the language of the input text"""
        # 간단한 한글 감지 (한글 유니코드 범위: AC00-D7A3)
        if any('\uAC00' <= char <= '\uD7A3' for char in text):
            return 'ko'
        return 'en'

    async def retrieve(self, query: str, filter: Optional[Dict[str, Any]] = None, top_k: int = 5) -> Dict[str, Any]:
        """Retrieve documents from the vector store."""
        try:
            print(f"\nProcessing query: {query}")
            
            docs = await self.vector_store.similarity_search(query, filter=filter, top_k=top_k)
            
            if not docs:
                return {
                    "source_documents": []
                }
            
            return {
                "source_documents": docs
            }
        except Exception as e:
            print(f"Error in retrieve: {e}")
            return {
                "source_documents": []
            }
        
    async def answer(self, query: str, filter: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Answer a query using the RAG chain."""
        try:
            # Detect language
            lang = self.detect_language(query)
            print(f"Detected language: {lang}")
            
            # Select prompt based on language
            prompt_template = self.prompts[lang]
            prompt = PromptTemplate(
                template=prompt_template,
                input_variables=["context", "question"]
            )

            chain = RetrievalQA.from_chain_type(
                llm=self.llm,
                chain_type="stuff",
                retriever=self.vector_store.get_retriever(),
                return_source_documents=True,
                chain_type_kwargs={
                    "prompt": prompt
                }
            )

            print("Running RetrievalQA chain...")
            result = await asyncio.to_thread(
                chain,
                {"query": query}
            )
            
            answer = result.get("result", "I don't have enough information to answer that question." if lang == 'en' else "질문에 답변하기에 충분한 정보가 없습니다.")
            source_documents = result.get("source_documents", [])
            
            print(f"Answer: {answer}")
            print(f"Number of source documents: {len(source_documents)}")
            
            return {
                "answer": answer,
                "source_documents": source_documents
            }
        except Exception as e:
            print(f"Error in answer_question: {e}")
            return {
                "answer": "An error occurred while processing your question." if lang == 'en' else "질문을 처리하는 중 오류가 발생했습니다.",
                "sources": []
            }
    
    async def process_and_store_documents(self, documents: list[Document], tenant_id: str) -> bool:
        """Process and store documents in the vector store with integrated image analysis."""
        try:
            print(f"\nProcessing {len(documents)} documents...")
            
            # 이미지 분석을 먼저 수행
            await self.process_document_images(documents)
            
            # 벡터 저장소에 저장 (이미지 분석 완료된 상태)
            return await self.vector_store.add_documents(documents, tenant_id)
            
        except Exception as e:
            print(f"Error in process_and_store_documents: {e}")
            return False

    def _image_index_from_id(self, image_id: str) -> Optional[int]:
        """image_id에서 페이지 내 이미지 인덱스 추출 (예: xxx_page12_img0 -> 0)"""
        if not image_id or "_img" not in image_id:
            return None
        try:
            part = image_id.split("_img")[-1]
            return int(part)
        except (IndexError, ValueError):
            return None

    def _get_image_page_number(self, image_info: Dict[str, Any]) -> Optional[int]:
        """이미지 메타데이터에서 페이지 번호 추출 (1-based). PDF: page_number, image_id 내 page 도 활용."""
        meta = image_info.get('metadata') or {}
        if meta.get('page_number') is not None:
            return int(meta['page_number'])
        # image_id 형식: {file_id}_page{num}_img{idx} (PDF)
        image_id = image_info.get('image_id', '')
        if '_page' in image_id:
            try:
                part = image_id.split('_page')[1]
                page_str = part.split('_')[0]
                return int(page_str)
            except (IndexError, ValueError):
                pass
        return None

    async def process_document_images(self, documents: list[Document]) -> None:
        """문서들의 이미지를 분석하고, 해당 이미지가 나오는 페이지/구간의 청크에만 설명 추가."""
        try:
            # 1) 고유 이미지 수집 (image_id 기준, 한 번만 분석)
            unique_images: Dict[str, Dict[str, Any]] = {}
            for doc in documents:
                if 'extracted_images' in doc.metadata and doc.metadata['extracted_images']:
                    for img in doc.metadata['extracted_images']:
                        iid = img.get('image_id')
                        if iid and iid not in unique_images:
                            unique_images[iid] = img
                # 단일 이미지 파일 (extracted_images 없는 경우)
                if 'image_url' in doc.metadata and doc.metadata.get('image_url'):
                    if not ('extracted_images' in doc.metadata and doc.metadata['extracted_images']):
                        image_info = {
                            'image_id': doc.metadata.get('file_id', doc.metadata.get('file_name', 'unknown')),
                            'image_url': doc.metadata['image_url'],
                            'metadata': {
                                'format': doc.metadata.get('file_type', 'png'),
                                'source_path': doc.metadata.get('file_name', 'unknown'),
                                'image_index': 0
                            }
                        }
                        unique_images[image_info['image_id']] = image_info
                        # 단일 이미지는 해당 doc에만 넣기 위해 doc 참조 보관 (아래에서 처리)
            unique_list = list(unique_images.values())
            if not unique_list:
                return

            # 2) 고유 이미지만 1회 분석
            print(f"Analyzing {len(unique_list)} unique images (once per document set)...")
            analyzed_list = await self.analyze_images_with_llm(unique_list)
            # image_id -> 분석 텍스트, 페이지(1-based)
            analysis_by_id: Dict[str, tuple[str, Optional[int]]] = {}
            for img in analyzed_list:
                iid = img.get('image_id')
                if not iid:
                    continue
                analysis_text = img.get('analysis') or ""
                page_num = None
                orig = unique_images.get(iid)
                if orig:
                    page_num = self._get_image_page_number(orig)
                analysis_by_id[iid] = (analysis_text, page_num)

            # 3) (page_1based, img_index) -> (image_id, analysis_text) 역방향 매핑 구성
            placeholder_map: Dict[tuple, tuple] = {}
            for iid, (analysis_text, page_1based) in analysis_by_id.items():
                img_index = self._image_index_from_id(iid)
                if page_1based is not None and img_index is not None and analysis_text:
                    placeholder_map[(page_1based, img_index)] = (iid, analysis_text)

            placeholder_re = re.compile(r"__IMAGE_PLACEHOLDER_p(\d+)_i(\d+)__")

            # 4) 청크별 처리
            for doc in documents:
                is_single_image_doc = (
                    doc.metadata.get('image_url')
                    and not (doc.metadata.get('extracted_images'))
                )

                # 단일 이미지 파일(PNG/JPG 등): 기존 방식으로 텍스트 끝에 추가
                if is_single_image_doc:
                    chunk_image_analyses = []
                    for iid, (analysis_text, _) in analysis_by_id.items():
                        if not analysis_text:
                            continue
                        doc_iid = doc.metadata.get('file_id', doc.metadata.get('file_name', 'unknown'))
                        if iid != doc_iid:
                            continue
                        chunk_image_analyses.append({
                            'image_id': iid,
                            'analysis': analysis_text,
                            'metadata': unique_images.get(iid, {}).get('metadata', {}),
                            'image_url': unique_images.get(iid, {}).get('image_url', '')
                        })
                    if chunk_image_analyses:
                        doc.page_content += "\n\n" + "\n\n".join(
                            f"[이미지]\n{a['analysis']}" for a in chunk_image_analyses
                        )
                        doc.metadata['image_analysis'] = chunk_image_analyses
                        doc.metadata['extracted_images'] = [
                            unique_images[a['image_id']] for a in chunk_image_analyses
                            if a.get('image_id') in unique_images
                        ]
                        doc.metadata['image_count'] = len(doc.metadata['extracted_images'])
                    else:
                        doc.metadata['extracted_images'] = []
                        doc.metadata['image_count'] = 0
                    continue

                # PDF: 플레이스홀더를 이미지 분석 텍스트로 치환
                placeholders_found = placeholder_re.findall(doc.page_content)
                chunk_image_analyses = []

                if placeholders_found:
                    for page_str, idx_str in placeholders_found:
                        entry = placeholder_map.get((int(page_str), int(idx_str)))
                        if entry:
                            iid, analysis_text = entry
                            chunk_image_analyses.append({
                                'image_id': iid,
                                'analysis': analysis_text,
                                'metadata': unique_images.get(iid, {}).get('metadata', {}),
                                'image_url': unique_images.get(iid, {}).get('image_url', '')
                            })

                    def make_replacer(pmap: Dict[tuple, tuple]):
                        def replacer(match: re.Match) -> str:
                            entry = pmap.get((int(match.group(1)), int(match.group(2))))
                            if entry:
                                _, analysis_text = entry
                                page = int(match.group(1))
                                idx = int(match.group(2))
                                return f"[이미지: {page}페이지 이미지{idx + 1}]\n{analysis_text}"
                            return ""
                        return replacer

                    doc.page_content = placeholder_re.sub(make_replacer(placeholder_map), doc.page_content)

                    if chunk_image_analyses:
                        doc.metadata['image_analysis'] = chunk_image_analyses
                        doc.metadata['extracted_images'] = [
                            unique_images[a['image_id']] for a in chunk_image_analyses
                            if a.get('image_id') in unique_images
                        ]
                        doc.metadata['image_count'] = len(doc.metadata['extracted_images'])
                        print(f"Replaced {len(chunk_image_analyses)} image placeholder(s) in chunk")
                    else:
                        doc.metadata['extracted_images'] = []
                        doc.metadata['image_count'] = 0
                else:
                    doc.metadata['extracted_images'] = []
                    doc.metadata['image_count'] = 0
        except Exception as e:
            print(f"Error in process_document_images: {e}")
            raise

    async def get_processed_files(self, tenant_id: str) -> List[str]:
        """Get list of already processed files for a tenant"""
        try:
            print(f"Getting processed files for tenant: {tenant_id}")
            result = await asyncio.to_thread(
                self.supabase.table('processed_files')
                .select('file_id')
                .eq('tenant_id', tenant_id)
                .execute
            )
            
            return [row['file_id'] for row in result.data]
        except Exception as e:
            print(f"Error getting processed files: {e}")
            return []

    async def save_processed_files(self, file_ids: List[str], tenant_id: str, file_names: List[str] = None) -> bool:
        """Save list of processed files"""
        try:
            # Prepare data for batch insert
            data = []
            for i, file_id in enumerate(file_ids):
                data.append({
                    'file_id': file_id,
                    'tenant_id': tenant_id,
                    'file_name': file_names[i] if file_names else None
                })
            
            # Batch insert
            await asyncio.to_thread(
                self.supabase.table('processed_files')
                .insert(data)
                .execute
            )
            
            return True
        except Exception as e:
            print(f"Error saving processed files: {e}")
            return False

    async def delete_processed_file(self, file_id: str, tenant_id: str) -> bool:
        """Delete a processed file record"""
        try:
            await asyncio.to_thread(
                self.supabase.table('processed_files')
                .delete()
                .eq('file_id', file_id)
                .eq('tenant_id', tenant_id)
                .execute
            )
            return True
        except Exception as e:
            print(f"Error deleting processed file: {e}")
            return False

    async def process_database_records(self, records: List[Dict[str, Any]], tenant_id: str, options: Optional[Dict[str, Any]] = None) -> bool:
        """Process database records and store them in the vector store."""
        try:
            print(f"\nProcessing {len(records)} database records...")
            
            documents = []
            for record in records:
                if 'output' not in record:
                    print(f"Warning: Record {record.get('id', 'unknown')} has no 'output' column")
                    continue
                    
                output_json = record['output']
                # Convert dictionary to formatted string
                output_text = "\n".join([f"{key}: {value}" for key, value in output_json.items()])
                
                metadata = {
                    "tenant_id": tenant_id,
                    "source_type": "database",
                    "created_at": record.get('created_at', ''),
                    "updated_at": record.get('updated_at', ''),
                    **options
                }
                
                # Create Document object
                doc = Document(
                    page_content=output_text,
                    metadata=metadata
                )
                documents.append(doc)
            
            # Store documents in vector store
            return await self.vector_store.add_documents(documents, tenant_id)
            
        except Exception as e:
            print(f"Error in process_database_records: {e}")
            return False

    async def analyze_images_with_llm(self, images_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """추출된 이미지를 LLM으로 분석 - URL 또는 base64 사용"""
        import base64
        import httpx
        from urllib.parse import urlparse
        
        analyzed_images = []
        
        for image_info in images_data:
            try:
                # Supabase Storage에서 업로드된 이미지의 URL 사용
                image_url = image_info.get('image_url')
                if not image_url:
                    print(f"Skipping image {image_info.get('image_id')}: No image URL found")
                    continue
                
                # 이미지 형식 감지
                image_format = image_info.get('metadata', {}).get('format', 'png')
                mime_type = f"image/{image_format.lower()}"
                
                # URL이 localhost인지 확인
                parsed_url = urlparse(image_url)
                is_localhost = parsed_url.hostname in ['localhost', '127.0.0.1', '0.0.0.0'] or (
                    parsed_url.hostname and 'localhost' in parsed_url.hostname
                )
                
                # 이미지 콘텐츠 준비
                if is_localhost:
                    # localhost인 경우 base64 인코딩 사용
                    print(f"Downloading image from localhost URL: {image_url}")
                    async with httpx.AsyncClient() as client:
                        image_response = await client.get(image_url)
                        image_response.raise_for_status()
                        image_bytes = image_response.content
                    
                    # base64 인코딩
                    image_base64 = base64.b64encode(image_bytes).decode('utf-8')
                    image_data_url = f"data:{mime_type};base64,{image_base64}"
                    
                    # OpenAI Vision API에 base64 데이터 전달
                    response = await self.llm.ainvoke([
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "이 이미지를 자세히 분석하고 설명해주세요. 문서의 일부라면 텍스트 내용, 차트, 그래프, 이미지 등을 포함하여 설명해주세요."
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": image_data_url
                                    }
                                }
                            ]
                        }
                    ])
                else:
                    # localhost가 아닌 경우 URL 사용
                    print(f"Using image URL: {image_url}")
                    response = await self.llm.ainvoke([
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "이 이미지를 자세히 분석하고 설명해주세요. 문서의 일부라면 텍스트 내용, 차트, 그래프, 이미지 등을 포함하여 설명해주세요."
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": image_url
                                    }
                                }
                            ]
                        }
                    ])
                
                analyzed_images.append({
                    'image_id': image_info['image_id'],
                    'analysis': response.content,
                    'metadata': image_info['metadata'],
                    'image_url': image_url
                })
                
                print(f"Successfully analyzed image {image_info.get('image_id')} from URL: {image_url}")
                
            except Exception as e:
                print(f"Error analyzing image {image_info.get('image_id')}: {e}")
                continue
        
        return analyzed_images

# Example usage
if __name__ == "__main__":
    rag = RAGChain()
    result = rag.answer(
        "What is the budget for Project A?",
        filter={"storage_type": "Local"}
    )
    print(f"Answer: {result['answer']}")
    print("\nSources:")
    for source in result["sources"]:
        print(f"- {source[:100]}...") 