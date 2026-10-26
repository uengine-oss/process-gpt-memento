"""
Document loader and processor
"""
import os
import uuid
import tempfile
import asyncio
import io
import zipfile
from typing import List, Optional, Dict, Any, AsyncIterator
from pathlib import Path
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

from process_gpt_document_parser import EncryptedDocumentError, ParseError, detect_format

from app.plugins.chunkers import get_chunker
from app.plugins.parsers import get_pdf_parser
from app.plugins.parsers.document_parser import parse_flowing
from app.services.document_blocks import BLOCKS_KEY
from app.services import pdf_render
from app.services.file_to_pdf import convert_to_pdf, FileToPdfError


def _load_xlsx_documents(data: bytes, file_name: str) -> List[Document]:
    """xlsx 바이트 → 시트별 Document (한 시트 = 한 페이지).

    셀은 행 단위로 탭 구분해 이어붙인다. 표 구조를 그대로 남겨야 grep/read_document_page 로
    "어느 시트 몇 번째 행" 을 사람이 알아볼 수 있다.
    """
    from openpyxl import load_workbook

    # 시트당 행 상한 — 수십만 행짜리 원장을 통째로 텍스트화하면 청킹 단계에서 메모리가 터진다.
    # 잘린 사실은 본문에 남긴다(조용히 줄이면 "다 읽었다"로 오해된다).
    max_rows = 20000

    wb = load_workbook(io.BytesIO(data), data_only=True, read_only=True)
    docs: List[Document] = []
    try:
        for idx, ws in enumerate(wb.worksheets, start=1):
            lines: List[str] = []
            truncated = False
            for row in ws.iter_rows(values_only=True):
                if len(lines) >= max_rows:
                    truncated = True
                    break
                cells = ["" if v is None else str(v).replace("\n", " ") for v in row]
                while cells and not cells[-1]:
                    cells.pop()
                if cells:
                    lines.append("\t".join(cells))
            if truncated:
                lines.append(f"[… 이 시트는 {max_rows}행까지만 수록됨]")
            if not lines:
                continue
            docs.append(Document(
                page_content=f"[시트: {ws.title}]\n" + "\n".join(lines),
                # page 는 PDF 파서와 같은 0-based — 저장 단계가 +1 한다.
                metadata={"source": file_name, "page": idx - 1, "sheet_name": ws.title},
            ))
    finally:
        wb.close()
    return docs


class DocumentProcessor:
    def __init__(self, chunk_size: int | None = None, chunk_overlap: int | None = None):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        # Chunking strategy is selected via chunkers/config.py (STRATEGY 값).
        # chunk_size/chunk_overlap이 None이면 config.py + PER_STRATEGY_OVERRIDES 값을 사용한다.
        self.chunker = get_chunker(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    async def _parse_via_pdf(self, data: bytes, file_name: str, file_extension: str) -> Optional[List[Document]]:
        """LibreOffice 로 PDF 를 만들어 PDF 파서로 읽는다. 못 하면 None."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as tmp:
            await asyncio.to_thread(tmp.write, data)
            tmp_path = tmp.name
        converted_pdf_path = None
        try:
            converted_pdf_path = await asyncio.to_thread(convert_to_pdf, tmp_path, os.path.dirname(tmp_path))
            with open(converted_pdf_path, "rb") as f:
                pdf_bytes = await asyncio.to_thread(f.read)
            return await get_pdf_parser().parse(pdf_bytes, f"{Path(file_name).stem}.pdf")
        except FileToPdfError as e:
            print(f"[parser] {file_name} PDF 변환 실패: {e}")
            return None
        finally:
            if converted_pdf_path and converted_pdf_path != tmp_path and os.path.exists(converted_pdf_path):
                await asyncio.to_thread(os.unlink, converted_pdf_path)
            await asyncio.to_thread(os.unlink, tmp_path)

    async def load_document(self, file_content: bytes, file_name: str) -> Optional[List[Document]]:
        """Async: Load a document from memory (BytesIO object)."""
        try:
            file_extension = os.path.splitext(file_name)[1].lower()

            documents = None

            # 텍스트 계열: 그대로 UTF-8 디코드하여 문서로 취급 (JSON/MD/코드 등)
            if file_extension in (
                '.txt', '.json', '.md', '.markdown', '.yaml', '.yml',
                '.xml', '.html', '.htm', '.csv', '.tsv', '.log', '.ini',
                '.toml', '.env', '.js', '.ts', '.jsx', '.tsx', '.py',
                '.java', '.go', '.rs', '.sql', '.sh', '.c', '.cpp', '.h',
            ):
                content = await asyncio.to_thread(file_content.read)
                if isinstance(content, bytes):
                    try:
                        content = content.decode('utf-8-sig')
                    except UnicodeDecodeError:
                        content = content.decode('utf-8', errors='replace')
                documents = [Document(page_content=content)]
            elif file_extension == '.pptx':
                # pptx → LibreOffice 로 PDF 변환 후 PDF 파서(슬라이드 이미지/도식도 Vision OCR).
                # 서버에 LibreOffice(soffice) 필요.
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pptx') as tmp:
                    await asyncio.to_thread(tmp.write, file_content.read())
                    tmp_path = tmp.name
                converted_pdf_path = None
                try:
                    try:
                        converted_pdf_path = await asyncio.to_thread(
                            convert_to_pdf, tmp_path, os.path.dirname(tmp_path)
                        )
                    except FileToPdfError as conv_err:
                        print(f"[pptx] LibreOffice PDF 변환 실패 for {file_name}: {conv_err}")
                        return None
                    with open(converted_pdf_path, "rb") as f:
                        pdf_bytes = await asyncio.to_thread(f.read)
                    documents = await get_pdf_parser().parse(
                        pdf_bytes, f"{Path(file_name).stem}.pdf"
                    )
                finally:
                    if (
                        converted_pdf_path
                        and converted_pdf_path != tmp_path
                        and os.path.exists(converted_pdf_path)
                    ):
                        await asyncio.to_thread(os.unlink, converted_pdf_path)
                    await asyncio.to_thread(os.unlink, tmp_path)
            elif file_extension == '.xlsx':
                # openpyxl 직접 사용. UnstructuredExcelLoader 는 unstructured/nltk 를 끌고 들어와
                # 첫 호출에서 프로세스가 멎는다(관측: 채팅 xlsx 첨부 시 서버 사망). 엑셀은 셀 격자라
                # 레이아웃 분석이 필요 없으므로 시트별 텍스트 덤프로 충분하다.
                data = await asyncio.to_thread(file_content.read)
                documents = await asyncio.to_thread(_load_xlsx_documents, data, file_name)
            elif file_extension == '.pdf':
                data = await asyncio.to_thread(file_content.read)
                documents = await get_pdf_parser().parse(data, file_name)
            elif file_extension in ('.hwp', '.hwpx', '.doc', '.docx'):
                data = await asyncio.to_thread(file_content.read)
                try:
                    # 확장자와 실제 형식이 다른 채로 도는 파일이 있다(HWP5 바이너리에 .hwpx)
                    file_extension = "." + detect_format(data, file_name)
                    documents = await parse_flowing(data, file_name)
                except EncryptedDocumentError as e:
                    print(f"[parser] 암호가 걸린 문서 {file_name}: {e}")
                    return None
                except ParseError as e:
                    print(f"[parser] {file_name} 읽기 실패, PDF 로 바꿔 다시 읽는다: {e}")
                    documents = await self._parse_via_pdf(data, file_name, file_extension)
                    if documents is None:
                        return None
            else:
                print(f"Unsupported file type: {file_extension}")
                return None

            # Add basic metadata including UUID
            doc_id = str(uuid.uuid4())  # Generate a single UUID for the document
            for doc in documents:
                doc.metadata.update({
                    "id": doc_id,
                    "source": file_name,
                    "file_name": file_name,
                    "file_type": file_extension[1:],
                    "language": "ko",
                    "content_length": len(doc.page_content)
                })
            
            return documents
        except Exception as e:
            print(f"Error loading document from memory {file_name}: {e}")
            return None

    @staticmethod
    def _attach_chunk_bbox(chunk) -> None:
        """청크의 start_index + 부모 blocks_json을 이용해 bbox 유니온을 계산해 붙인다.

        결과: chunk.metadata['bboxes_json'] = '[{"page": N, "bbox": [x0,y0,x1,y1],
        "page_width": W, "page_height": H}]' (Chroma primitive 제약 때문에 문자열로).
        PDF 아닌 문서는 blocks_json이 없으므로 아무것도 하지 않음.
        """
        import json as _json
        blocks_json = chunk.metadata.get("blocks_json")
        if not blocks_json:
            return
        try:
            blocks = _json.loads(blocks_json)
        except Exception:
            return
        if not blocks:
            return
        start = chunk.metadata.get("start_index")
        if not isinstance(start, int) or start < 0:
            return
        end = start + len(chunk.page_content or "")
        # 청크 범위 [start, end)와 겹치는 블록들의 bbox 유니온 계산
        x0 = y0 = float("inf")
        x1 = y1 = float("-inf")
        matched = False
        for blk in blocks:
            b_off = blk.get("offset")
            b_len = blk.get("length")
            bb = blk.get("bbox")
            if not isinstance(b_off, int) or not isinstance(b_len, int) or not isinstance(bb, list) or len(bb) != 4:
                continue
            b_end = b_off + b_len
            # 겹침 판정
            if b_end <= start or b_off >= end:
                continue
            matched = True
            x0 = min(x0, float(bb[0]))
            y0 = min(y0, float(bb[1]))
            x1 = max(x1, float(bb[2]))
            y1 = max(y1, float(bb[3]))
        if not matched:
            return
        page_num = chunk.metadata.get("page")
        page_w = chunk.metadata.get("page_width")
        page_h = chunk.metadata.get("page_height")
        entry = {
            "page": int(page_num) if page_num is not None else None,
            "bbox": [round(x0, 2), round(y0, 2), round(x1, 2), round(y1, 2)],
        }
        if page_w is not None:
            entry["page_width"] = float(page_w)
        if page_h is not None:
            entry["page_height"] = float(page_h)
        chunk.metadata["bboxes_json"] = _json.dumps([entry], ensure_ascii=False)
        # 원본 blocks_json은 청크 메타에 계속 남아있으면 저장 용량 낭비 → 제거
        chunk.metadata.pop("blocks_json", None)

    async def process_documents(self, documents: List[Document], metadata: dict = None) -> List[Document]:
        """Async: Process documents by splitting them into chunks and adding metadata."""
        try:
            print(f"[chunk] {len(documents)}개 문서(페이지) 청킹 시작...")
            # Add additional metadata if provided
            if metadata:
                for doc in documents:
                    doc.metadata.update(metadata)
            
            # Split documents into chunks (strategy pluggable via CHUNKER_STRATEGY)
            chunks = await self.chunker.split(documents)
            for chunk in chunks:
                chunk.metadata.pop(BLOCKS_KEY, None)  # 블록 목록은 document_blocks 로만 간다

            # Add chunk information to metadata
            for i, chunk in enumerate(chunks):
                chunk_id = str(uuid.uuid4())
                chunk.metadata.update({
                    "chunk_id": chunk_id,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    "content_length": len(chunk.page_content or ""),
                })
                # PDF 등: 0-based page가 있으면 1-based page_number 추가 (이미지 page_number와 일치)
                if "page" in chunk.metadata and chunk.metadata["page"] is not None:
                    try:
                        chunk.metadata["page_number"] = int(chunk.metadata["page"]) + 1
                    except (TypeError, ValueError):
                        pass

                # PDF bbox 역산: 부모 페이지의 blocks_json + chunk의 start_index로
                # 어떤 블록(들)이 청크에 포함됐는지 찾아 bbox 유니온을 계산.
                try:
                    self._attach_chunk_bbox(chunk)
                except Exception as bbox_err:
                    print(f"[bbox] 청크 bbox 계산 실패: {bbox_err}")

            return chunks
        except Exception as e:
            print(f"Error processing documents: {e}")
            return []

    def process_directory(self, directory_path: str, metadata: dict = None) -> List[Document]:
        """Process all documents in a directory."""
        all_documents = []
        
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                
                # Read file content into memory
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                documents = self.load_document(file_content, file_path)
                
                if documents:
                    print(f"Loaded {len(documents)} documents from {file_path}")
                    all_documents.extend(documents)
        
        if all_documents:
            print(f"Processing {len(all_documents)} documents...")
            chunks = self.process_documents(all_documents, metadata)
            print(f"Created {len(chunks)} chunks")
            return chunks
        
        return []

    async def extract_images_from_document(self, file_content: bytes, file_name: str, file_id: str) -> List[Dict[str, Any]]:
        """문서에서 이미지 추출 (PDF, DOCX, PPTX 지원) - 재사용 가능한 버전"""
        extracted_images = []
        
        print(f"Starting image extraction for file: {file_name}")
        
        # 파일 확장자 확인
        file_extension = Path(file_name).suffix.lower()
        
        if file_extension == '.pdf':
            print("Processing PDF file for image extraction...")
            extracted_images = await self._extract_images_from_pdf(file_content, file_name, file_id)
        elif file_extension == '.docx':
            print("Processing DOCX file for image extraction...")
            extracted_images = await self._extract_images_from_docx(file_content, file_name, file_id)
        elif file_extension == '.pptx':
            print("Processing PPTX file for image extraction...")
            extracted_images = await self._extract_images_from_pptx(file_content, file_name, file_id)
        else:
            print(f"File type {file_extension} not supported for image extraction")
            
        print(f"Extracted {len(extracted_images)} images from {file_name}")
        return extracted_images

    async def _extract_images_from_pdf(self, file_content: bytes, file_name: str, file_id: str) -> List[Dict[str, Any]]:
        """PDF 파일에서 이미지 추출"""
        return [img async for img in self._iter_pdf_images(file_content, file_name, file_id)]

    async def _extract_images_from_docx(self, file_content: bytes, file_name: str, file_id: str) -> List[Dict[str, Any]]:
        """DOCX 파일에서 이미지 추출"""
        extracted_images = []
        
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as tmp_file:
                tmp_file.write(file_content)
                tmp_path = tmp_file.name
            
            try:
                with zipfile.ZipFile(tmp_path, 'r') as docx_zip:
                    image_files = [f for f in docx_zip.namelist() if f.startswith('word/media/')]
                    
                    for img_index, image_path in enumerate(image_files):
                        try:
                            # 이미지 데이터 추출
                            image_data = docx_zip.read(image_path)
                            
                            # 파일 확장자 추출
                            file_extension = Path(image_path).suffix.lower()
                            if not file_extension:
                                # MIME 타입으로 판단
                                if image_data.startswith(b'\xff\xd8\xff'):
                                    file_extension = '.jpg'
                                elif image_data.startswith(b'\x89PNG\r\n\x1a\n'):
                                    file_extension = '.png'
                                elif image_data.startswith(b'GIF8'):
                                    file_extension = '.gif'
                                else:
                                    file_extension = '.png'
                            
                            # 이미지 이름 생성
                            image_name = f"{file_id}_img{img_index+1}{file_extension}"
                            
                            # 이미지 메타데이터 생성
                            image_metadata = {
                                'format': file_extension[1:],
                                'source_path': image_path,
                                'image_index': img_index + 1
                            }
                            
                            extracted_images.append({
                                'image_id': f"{file_id}_img{img_index+1}",
                                'image_name': image_name,
                                'image_data': image_data,
                                'metadata': image_metadata
                            })
                            
                        except Exception as e:
                            print(f"Error extracting image {image_path}: {e}")
                            continue
                            
            finally:
                os.unlink(tmp_path)
                
        except Exception as e:
            print(f"Error processing DOCX file {file_name}: {e}")
            
        return extracted_images

    async def _extract_images_from_pptx(self, file_content: bytes, file_name: str, file_id: str) -> List[Dict[str, Any]]:
        """PPTX 파일에서 이미지 추출"""
        extracted_images = []
        
        try:
            print(f"Starting PPTX image extraction for {file_name}")
            
            # 임시 파일로 저장
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pptx') as tmp_file:
                tmp_file.write(file_content)
                tmp_path = tmp_file.name
            
            try:
                # PPTX는 ZIP 파일이므로 압축 해제
                with zipfile.ZipFile(tmp_path, 'r') as pptx_zip:
                    # 이미지 파일들 찾기
                    image_files = [f for f in pptx_zip.namelist() if f.startswith('ppt/media/')]
                    print(f"Found {len(image_files)} image files in PPTX")
                    
                    for img_index, image_path in enumerate(image_files):
                        try:
                            print(f"Processing image {img_index + 1}: {image_path}")
                            
                            # 이미지 데이터 추출
                            image_data = pptx_zip.read(image_path)
                            
                            # 파일 확장자 추출
                            file_extension = Path(image_path).suffix.lower()
                            if not file_extension:
                                # 확장자가 없는 경우 MIME 타입으로 판단
                                if image_data.startswith(b'\xff\xd8\xff'):
                                    file_extension = '.jpg'
                                elif image_data.startswith(b'\x89PNG\r\n\x1a\n'):
                                    file_extension = '.png'
                                elif image_data.startswith(b'GIF8'):
                                    file_extension = '.gif'
                                else:
                                    file_extension = '.png'  # 기본값
                            
                            print(f"Detected image format: {file_extension}")
                            
                            # 이미지 이름 생성
                            image_name = f"{file_id}_slide_img{img_index+1}{file_extension}"
                            
                            # 이미지 메타데이터 생성
                            image_metadata = {
                                'format': file_extension[1:],  # 확장자에서 점 제거
                                'source_path': image_path,
                                'slide_index': img_index + 1
                            }
                            
                            extracted_images.append({
                                'image_id': f"{file_id}_slide_img{img_index+1}",
                                'image_name': image_name,
                                'image_data': image_data,
                                'metadata': image_metadata
                            })
                            
                            print(f"Successfully extracted image {img_index + 1}")
                            
                        except Exception as e:
                            print(f"Error extracting image {image_path}: {e}")
                            continue
                            
            finally:
                os.unlink(tmp_path)
                
        except Exception as e:
            print(f"Error processing PPTX file {file_name}: {e}")
            
        return extracted_images

    async def iter_extract_images_from_document(
        self, file_content: bytes, file_name: str, file_id: str
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield one image at a time so callers can process in batches without holding all bytes."""
        file_extension = Path(file_name).suffix.lower()
        if file_extension == '.pdf':
            async for img in self._iter_pdf_images(file_content, file_name, file_id):
                yield img
        elif file_extension == '.docx':
            async for img in self._iter_zip_images(file_content, file_id, suffix='.docx',
                                                   prefix='word/media/', id_tpl='{file_id}_img{i}'):
                yield img
        elif file_extension == '.pptx':
            async for img in self._iter_zip_images(file_content, file_id, suffix='.pptx',
                                                   prefix='ppt/media/', id_tpl='{file_id}_slide_img{i}'):
                yield img

    async def _iter_pdf_images(
        self, file_content: bytes, file_name: str, file_id: str
    ) -> AsyncIterator[Dict[str, Any]]:
        try:
            images = await asyncio.to_thread(lambda: list(pdf_render.iter_images(file_content)))
        except Exception as e:
            print(f"Error processing PDF file {file_name}: {e}")
            return
        for page_index, img_index, image_bytes, ext in images:
            image_id = f"{file_id}_page{page_index + 1}_img{img_index}"
            yield {
                'image_id': image_id,
                'image_name': f"{image_id}.{ext}",
                'image_data': image_bytes,
                'metadata': {
                    'format': ext,
                    'source_path': file_name,
                    'page_number': page_index + 1,
                    'image_index': img_index,
                },
            }

    async def _iter_zip_images(
        self, file_content: bytes, file_id: str, suffix: str, prefix: str, id_tpl: str
    ) -> AsyncIterator[Dict[str, Any]]:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(file_content)
            tmp_path = tmp.name
        try:
            with zipfile.ZipFile(tmp_path, 'r') as zf:
                image_files = [f for f in zf.namelist() if f.startswith(prefix)]
                for i, image_path in enumerate(image_files, start=1):
                    try:
                        data = zf.read(image_path)
                        ext = Path(image_path).suffix.lower()
                        if not ext:
                            if data.startswith(b'\xff\xd8\xff'):
                                ext = '.jpg'
                            elif data.startswith(b'\x89PNG\r\n\x1a\n'):
                                ext = '.png'
                            elif data.startswith(b'GIF8'):
                                ext = '.gif'
                            else:
                                ext = '.png'
                        image_id = id_tpl.format(file_id=file_id, i=i)
                        yield {
                            'image_id': image_id,
                            'image_name': f"{image_id}{ext}",
                            'image_data': data,
                            'metadata': {
                                'format': ext[1:],
                                'source_path': image_path,
                                'image_index': i,
                            },
                        }
                    except Exception as e:
                        print(f"Error extracting image {image_path}: {e}")
                        continue
        finally:
            os.unlink(tmp_path)

    async def extract_and_upload_images_batched(
        self,
        file_content: bytes,
        file_name: str,
        file_id: str,
        tenant_id: str,
        batch_size: int = 15,
    ) -> List[Dict[str, Any]]:
        """Extract and upload images in batches; raw bytes released between batches."""
        from app.core import config
        if not config.image_analysis_enabled():
            print("Skipping image extraction/upload (vision disabled)")
            return []

        from app.storage.image_storage import get_image_storage_utils
        storage = get_image_storage_utils()
        folder = f"extracted_images/{tenant_id}/{file_id}"

        uploaded: List[Dict[str, Any]] = []
        batch: List[Dict[str, Any]] = []
        total = 0

        async def flush() -> None:
            nonlocal total
            if not batch:
                return
            results = await asyncio.gather(*[
                storage.upload_image_to_storage(img['image_data'], img['image_name'], folder)
                for img in batch
            ], return_exceptions=True)
            for img, result in zip(batch, results):
                if isinstance(result, Exception) or not result:
                    print(f"Error uploading {img['image_name']}: {result}")
                    continue
                uploaded.append({
                    'image_id': img['image_id'],
                    'image_name': img['image_name'],
                    'image_url': result.get('public_url'),
                    'metadata': img['metadata'],
                })
            total += len(batch)
            print(f"Uploaded {total} images (batch of {len(batch)})")
            batch.clear()

        async for img in self.iter_extract_images_from_document(file_content, file_name, file_id):
            batch.append(img)
            if len(batch) >= batch_size:
                await flush()
        await flush()
        return uploaded


_document_processor_instance: Optional["DocumentProcessor"] = None


def get_document_processor() -> "DocumentProcessor":
    global _document_processor_instance
    if _document_processor_instance is None:
        _document_processor_instance = DocumentProcessor()
    return _document_processor_instance


# Example usage
if __name__ == "__main__":
    processor = DocumentProcessor()
    documents = processor.process_directory("./documents")
    print(f"Processed {len(documents)} document chunks")
