import os
import re
from typing import List, Dict, Any, Optional
import asyncio
from langchain.schema import Document
from app.core.env_loader import load_project_dotenv
from app.services.vector_store import VectorStoreManager, get_vector_store
from app.services.llm import create_llm
from app.plugins.retrievers import get_retriever

load_project_dotenv(override=True)

import logging as _logging
_logger = _logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


# ── 비전(이미지 분석) 안전장치 설정 ──────────────────────────────────────────
# 이미지 분석은 문서당 이미지 수만큼 vision LLM 을 호출한다. 동시성을 올리면 빨라지지만
# 비전 서버 과부하 위험 → (1) 프로세스 *전역* in-flight 상한 세마포어로 총량을 묶고,
# (2) 개별 이미지 실패는 transient 면 백오프 재시도, 최종 실패는 스킵하되 로그로 표면화한다.
# (여러 파일이 동시에 인덱싱돼도 비전 서버로 나가는 총 동시요청은 _VISION_MAX_INFLIGHT 이하)
_VISION_MAX_INFLIGHT = _int_env("MEMENTO_VISION_MAX_INFLIGHT", 8)   # 전역 총 동시 호출 상한
_VISION_RETRIES = _int_env("MEMENTO_VISION_MAX_RETRIES", 2)         # 개별 이미지 transient 재시도
_vision_sem: Optional["asyncio.Semaphore"] = None


def _get_vision_sem() -> "asyncio.Semaphore":
    global _vision_sem
    if _vision_sem is None:
        _vision_sem = asyncio.Semaphore(_VISION_MAX_INFLIGHT)
    return _vision_sem


def _is_transient_err(msg: str) -> bool:
    m = (msg or "").lower()
    return any(k in m for k in (
        "429", "rate limit", "too many requests", "timeout", "timed out",
        "502", "503", "504", "connection", "econnreset", "temporarily", "overload", "424",
    ))


class RAGChain:
    def __init__(self):
        print("Initializing RAG Chain...")

        llm_api_key = (
            os.getenv("LLM_API_KEY")
            or os.getenv("LLM_PROXY_API_KEY")
            or os.getenv("OPENROUTER_API_KEY")
            or os.getenv("OPENAI_API_KEY")
        )
        if not llm_api_key:
            raise ValueError(
                "No LLM API key found. Set one of: "
                "LLM_API_KEY, LLM_PROXY_API_KEY, OPENROUTER_API_KEY, OPENAI_API_KEY"
            )
        
        # Initialize proxy-routed LLM via shared helper
        print("Initializing proxy-routed LLM...")
        self.llm = create_llm(temperature=0.0)
        print("Proxy-routed LLM initialized successfully")
        
        self.vector_store = get_vector_store()
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
        """
        Retrieve documents from the vector store.

        실제 검색 전략은 retrievers/config.py의 STRATEGY로 선택되며
        (plain / multi_query / hyde / rag_fusion / rewrite), 여기서는
        해당 retriever에게 위임만 한다.
        """
        try:
            print(f"\nProcessing query: {query}")

            retriever = get_retriever(top_k=top_k)
            docs = await retriever.retrieve(
                query,
                self.vector_store,
                filter=filter,
                top_k=top_k,
            )

            if not docs:
                return {"source_documents": []}

            return {"source_documents": docs}
        except Exception as e:
            print(f"Error in retrieve: {e}")
            return {"source_documents": []}
        
    def _format_context_documents(self, source_documents: List[Document]) -> str:
        """Format retrieved documents into a compact context block for the LLM."""
        context_parts: List[str] = []

        for index, doc in enumerate(source_documents, start=1):
            metadata = doc.metadata or {}
            file_name = metadata.get("file_name") or metadata.get("source") or "unknown"
            section_title = metadata.get("section_title") or ""
            page_number = metadata.get("page_number") or metadata.get("page")
            chunk_index = metadata.get("chunk_index")

            header_parts = [f"문서 {index}", f"파일: {file_name}"]
            if section_title:
                header_parts.append(f"섹션: {section_title}")
            if page_number is not None:
                header_parts.append(f"페이지: {page_number}")
            if chunk_index is not None:
                header_parts.append(f"청크: {chunk_index}")

            context_parts.append(
                "\n".join(
                    [
                        " | ".join(header_parts),
                        doc.page_content or "",
                    ]
                ).strip()
            )

        return "\n\n".join(part for part in context_parts if part).strip()

    async def answer(
        self,
        query: str,
        filter: Optional[Dict[str, Any]] = None,
        top_k: int = 5,
    ) -> Dict[str, Any]:
        """Answer a query using the RAG chain."""
        lang = self.detect_language(query)
        try:
            print(f"Detected language: {lang}")

            prompt_template = self.prompts[lang]
            retrieval_result = await self.retrieve(query, filter=filter, top_k=top_k)
            source_documents = retrieval_result.get("source_documents", [])

            if not source_documents:
                return {
                    "answer": (
                        "I don't have enough information to answer that question."
                        if lang == "en"
                        else "질문에 답변하기에 충분한 정보가 없습니다."
                    ),
                    "source_documents": [],
                }

            context = self._format_context_documents(source_documents)
            final_prompt = prompt_template.format(context=context, question=query)

            print("Running direct RAG prompt with retrieved context...")
            response = await self.llm.ainvoke(final_prompt)
            answer = getattr(response, "content", response)
            if not isinstance(answer, str):
                answer = str(answer)

            print(f"Answer: {answer}")
            print(f"Number of source documents: {len(source_documents)}")

            return {
                "answer": answer,
                "source_documents": source_documents,
            }
        except Exception as e:
            print(f"Error in answer_question: {e}")
            return {
                "answer": (
                    "An error occurred while processing your question."
                    if lang == "en"
                    else "질문을 처리하는 중 오류가 발생했습니다."
                ),
                "source_documents": [],
            }
    
    async def process_and_store_documents(self, documents: list[Document], tenant_id: str) -> bool:
        """Process and store documents in the vector store with integrated image analysis."""
        try:
            print(f"\n[index] {len(documents)}개 청크 임베딩·벡터스토어 저장 시작...")

            try:
                from main import log_memory_snapshot
                log_memory_snapshot(f"embed-before:{tenant_id}")
            except Exception:
                pass

            await self.process_document_images(documents)
            result = await self.vector_store.add_documents(documents, tenant_id)

            try:
                from main import log_memory_snapshot
                log_memory_snapshot(f"embed-after:{tenant_id}")
            except Exception:
                pass

            return result

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
        from app.core import config
        if not config.image_analysis_enabled():
            # image_analysis 비활성 = env MEMENTO_IMAGE_ANALYSIS=false 이거나 provider 가 vision 미지원.
            # (PDF 페이지/영역 vision 은 별개 게이트 PDF_VISION_ENABLED — 이 skip 과 무관하게 동작)
            reason = "MEMENTO_IMAGE_ANALYSIS=false" if os.getenv("MEMENTO_IMAGE_ANALYSIS") else "provider vision 미지원"
            print(f"[image-analysis] 임베디드 이미지 재분석 skip ({reason})")
            return
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
        """Save list of processed files (멱등 — 재처리/이중 호출 시 덮어쓰기).

        processed_files 는 (file_id, tenant_id) unique 제약이 있어 plain insert 는 같은 파일을
        다시 처리할 때 23505(duplicate key)로 터진다. 세션 첨부는 같은 storage 파일이 결정적
        file_id(session/{tenant}/{uuid}) 로 매핑돼 재업로드/이중 호출이 흔하므로 upsert 로 저장한다.
        """
        try:
            # Prepare data for batch upsert
            data = []
            for i, file_id in enumerate(file_ids):
                data.append({
                    'file_id': file_id,
                    'tenant_id': tenant_id,
                    'file_name': file_names[i] if file_names else None
                })

            # Batch upsert — 중복 (file_id, tenant_id) 는 갱신(파일명 최신화), 신규는 삽입
            await asyncio.to_thread(
                self.supabase.table('processed_files')
                .upsert(data, on_conflict='file_id,tenant_id')
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

    async def analyze_images_with_llm(
        self,
        images_data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Vision 분석 — 전역 세마포어(_VISION_MAX_INFLIGHT)로 비전 서버 총 동시요청을 상한.

        여러 파일이 동시에 인덱싱돼도 비전 서버로 나가는 총 in-flight 는 전역 상한 이하로 유지된다.
        개별 이미지 실패는 _analyze_single_image 내부에서 transient 재시도 후 최종 실패 시 None 반환.
        """
        total = len(images_data)
        if not total:
            return []
        sem = _get_vision_sem()

        async def analyze_one(image_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            async with sem:
                return await self._analyze_single_image(image_info)

        results = await asyncio.gather(
            *[analyze_one(img) for img in images_data],
            return_exceptions=True,
        )
        analyzed: List[Dict[str, Any]] = []
        failed = 0
        for r in results:
            if isinstance(r, dict):
                analyzed.append(r)
            else:
                failed += 1
        # 조용한 손실 방지 — 스킵된 이미지 수를 표면화(동시성 과대/서버 과부하 신호)
        if failed:
            _logger.warning("[vision] 이미지 분석 스킵 %d/%d (transient 재시도 후에도 실패)", failed, total)
        print(f"Analyzed {len(analyzed)}/{total} images (skipped {failed})")
        return analyzed

    async def _analyze_single_image(self, image_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        import base64
        import httpx
        from urllib.parse import urlparse

        image_url = image_info.get('image_url')
        if not image_url:
            return None

        image_format = image_info.get('metadata', {}).get('format', 'png')
        mime_type = f"image/{image_format.lower()}"
        parsed_url = urlparse(image_url)
        is_localhost = parsed_url.hostname in ['localhost', '127.0.0.1', '0.0.0.0'] or (
            parsed_url.hostname and 'localhost' in parsed_url.hostname
        )
        prompt_text = "이 이미지를 자세히 분석하고 설명해주세요. 문서의 일부라면 텍스트 내용, 차트, 그래프, 이미지 등을 포함하여 설명해주세요."

        # transient(429/5xx/timeout/OOM) 실패는 지수 백오프로 재시도, 최종 실패 시 None(스킵).
        last_err = None
        for attempt in range(_VISION_RETRIES + 1):
            try:
                if is_localhost:
                    async with httpx.AsyncClient() as client:
                        image_response = await client.get(image_url)
                        image_response.raise_for_status()
                        image_bytes = image_response.content
                    image_b64 = base64.b64encode(image_bytes).decode('utf-8')
                    url_payload = f"data:{mime_type};base64,{image_b64}"
                else:
                    url_payload = image_url

                response = await self.llm.ainvoke([{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt_text},
                        {"type": "image_url", "image_url": {"url": url_payload}},
                    ],
                }])

                return {
                    'image_id': image_info['image_id'],
                    'analysis': response.content,
                    'metadata': image_info['metadata'],
                    'image_url': image_url,
                }
            except Exception as e:
                last_err = e
                if _is_transient_err(str(e)) and attempt < _VISION_RETRIES:
                    await asyncio.sleep(min(20.0, 1.5 * (2 ** attempt)))
                    continue
                break
        print(f"Error analyzing image {image_info.get('image_id')}: {last_err}")
        return None


_rag_chain_instance: Optional["RAGChain"] = None
_rag_chain_lock = __import__("threading").Lock()


def get_rag_chain() -> "RAGChain":
    global _rag_chain_instance
    if _rag_chain_instance is None:
        with _rag_chain_lock:
            if _rag_chain_instance is None:
                _rag_chain_instance = RAGChain()
    return _rag_chain_instance


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
