"""
Document loader and processor
"""
import os
import uuid
from typing import List, Optional
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import (
    UnstructuredWordDocumentLoader,
    UnstructuredPowerPointLoader,
    UnstructuredExcelLoader,
    UnstructuredFileLoader,
    PyPDFLoader,
    TextLoader
)

class DocumentProcessor:
    def __init__(self, chunk_size: int = 2000, chunk_overlap: int = 400):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            separators=["\n\n", "\n", ".", "!", "?", " ", ""],
            length_function=len,
            is_separator_regex=False
        )

    def load_document(self, file_content: bytes, file_name: str) -> Optional[List[Document]]:
        """Load a document from memory (BytesIO object)."""
        try:
            file_extension = os.path.splitext(file_name)[1].lower()
            
            if file_extension == '.txt':
                content = file_content.read().decode('utf-8-sig')
                documents = [Document(page_content=content)]
            elif file_extension == '.docx':
                loader = UnstructuredWordDocumentLoader(file_content, mode="single")
                documents = loader.load()
            elif file_extension == '.pptx':
                loader = UnstructuredPowerPointLoader(file_content, mode="single")
                documents = loader.load()
            elif file_extension == '.xlsx':
                loader = UnstructuredExcelLoader(file_content, mode="single")
                documents = loader.load()
            elif file_extension == '.pdf':
                loader = PyPDFLoader(file_content)
                documents = loader.load()
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

    def process_documents(self, documents: List[Document], metadata: dict = None) -> List[Document]:
        """Process documents by splitting them into chunks and adding metadata."""
        try:
            print(f"Processing {len(documents)} documents...")
            # Add additional metadata if provided
            if metadata:
                for doc in documents:
                    doc.metadata.update(metadata)
            
            # Split documents into chunks
            chunks = self.text_splitter.split_documents(documents)
            
            # Add chunk information to metadata
            for i, chunk in enumerate(chunks):
                chunk_id = str(uuid.uuid4())  # Generate a unique ID for each chunk
                chunk.metadata.update({
                    "chunk_id": chunk_id,
                    "chunk_index": i,
                    "total_chunks": len(chunks)
                })
            
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

# Example usage
if __name__ == "__main__":
    processor = DocumentProcessor()
    documents = processor.process_directory("./documents")
    print(f"Processed {len(documents)} document chunks") 