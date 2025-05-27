import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.chains.retrieval_qa.base import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain.schema import Document
from vector_store import VectorStoreManager

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
            model_name="gpt-3.5-turbo",
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

    def answer(self, query: str, filter: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Answer a query using the RAG chain."""
        try:
            print(f"\nProcessing query: {query}")
            
            # Detect language
            lang = self.detect_language(query)
            print(f"Detected language: {lang}")
            
            # Select prompt based on language
            prompt_template = self.prompts[lang]
            prompt = PromptTemplate(
                template=prompt_template,
                input_variables=["context", "question"]
            )

            # First try direct similarity search
            docs = self.vector_store.similarity_search(query, filter=filter)
            
            if not docs:
                return {
                    "answer": "I don't have enough information to answer that question." if lang == 'en' else "질문에 답변하기에 충분한 정보가 없습니다.",
                    "source_documents": []
                }

            # Create chain with language-specific prompt
            chain = RetrievalQA.from_chain_type(
                llm=self.llm,
                chain_type="stuff",
                retriever=self.vector_store.get_retriever(),
                return_source_documents=True,
                chain_type_kwargs={
                    "prompt": prompt
                }
            )

            # Run the chain
            print("Running RetrievalQA chain...")
            result = chain.invoke({"query": query})
            
            # Extract answer and sources
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
    
    def process_and_store_documents(self, documents: list[Document], tenant_id: str) -> bool:
        """Process and store documents in the vector store."""
        try:
            print(f"\nProcessing {len(documents)} documents...")
            return self.vector_store.add_documents(documents, tenant_id)
        except Exception as e:
            print(f"Error in process_and_store_documents: {e}")
            return False

    def get_processed_files(self, tenant_id: str) -> List[str]:
        """Get list of already processed files for a tenant"""
        try:
            result = self.supabase.table('processed_files') \
                .select('file_id') \
                .eq('tenant_id', tenant_id) \
                .execute()
            
            return [row['file_id'] for row in result.data]
        except Exception as e:
            print(f"Error getting processed files: {e}")
            return []

    def save_processed_files(self, file_ids: List[str], tenant_id: str, file_names: List[str] = None) -> bool:
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
            self.supabase.table('processed_files') \
                .insert(data) \
                .execute()
            
            return True
        except Exception as e:
            print(f"Error saving processed files: {e}")
            return False

    def delete_processed_file(self, file_id: str, tenant_id: str) -> bool:
        """Delete a processed file record"""
        try:
            self.supabase.table('processed_files') \
                .delete() \
                .eq('file_id', file_id) \
                .eq('tenant_id', tenant_id) \
                .execute()
            return True
        except Exception as e:
            print(f"Error deleting processed file: {e}")
            return False

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