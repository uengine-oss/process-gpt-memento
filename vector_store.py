from typing import List, Dict, Any, Optional
import os
import uuid
from dotenv import load_dotenv
from supabase import create_client
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores.supabase import SupabaseVectorStore
from langchain.schema import Document
import asyncio

class VectorStoreManager:
    def __init__(self):
        # Load environment variables from .env file only
        load_dotenv(override=True)
        
        # Check for required environment variables
        required_vars = ['SUPABASE_URL', 'SUPABASE_KEY', 'OPENAI_API_KEY']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables in .env file: {', '.join(missing_vars)}")

        # Initialize Supabase client
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        try:
            print("Initializing Supabase client...")
            self.supabase = create_client(supabase_url, supabase_key)
            print("Supabase client initialized successfully")
        except Exception as e:
            print(f"Error initializing Supabase client: {e}")
            raise

        # Initialize OpenAI embeddings
        print("Initializing OpenAI embeddings...")
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        print("OpenAI embeddings initialized successfully")

        # Initialize Supabase vector store
        print("Initializing Supabase vector store...")
        self.vector_store = SupabaseVectorStore(
            client=self.supabase,
            embedding=self.embeddings,
            table_name="documents",
            query_name="match_documents",
            chunk_size=1000
        )
        print("Supabase vector store initialized successfully")
    
    async def add_documents(self, documents: List[Document], tenant_id: str) -> bool:
        """Add documents to the vector store asynchronously."""
        try:
            print(f"Adding {len(documents)} documents to vector store...")
            return await asyncio.to_thread(self._add_documents_sync, documents, tenant_id)
        except Exception as e:
            print(f"Error adding documents to vector store: {e}")
            return False

    def _add_documents_sync(self, documents: List[Document], tenant_id: str) -> bool:
        try:
            for doc in documents:
                if doc.metadata is None:
                    doc.metadata = {}
                if 'id' not in doc.metadata:
                    doc.metadata['id'] = str(uuid.uuid4())
                if 'type' not in doc.metadata:
                    doc.metadata['type'] = "upload_file"
                if 'tenant_id' not in doc.metadata:
                    doc.metadata['tenant_id'] = tenant_id
            texts = [doc.page_content for doc in documents]
            metadatas = [doc.metadata for doc in documents]
            embeddings = self.embeddings.embed_documents(texts)
            for i, (text, metadata, embedding) in enumerate(zip(texts, metadatas, embeddings)):
                try:
                    self.supabase.table("documents").insert({
                        "id": str(uuid.uuid4()),
                        "content": text,
                        "metadata": metadata,
                        "embedding": embedding
                    }).execute()
                    print(f"Successfully inserted document {i+1}/{len(documents)}")
                except Exception as e:
                    print(f"Error inserting document {i+1}: {e}")
                    continue
            return True
        except Exception as e:
            print(f"Error adding documents to vector store: {e}")
            return False

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
            if filter:
                print(f"Using filter: {filter}")
                if 'type' not in filter:
                    filter['type'] = "upload_file"
            else:
                filter = {"type": "upload_file"}
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
        # print("Creating retriever with kwargs:", kwargs)
        return self.vector_store.as_retriever(search_kwargs={"k": 5})

# Example usage
if __name__ == "__main__":
    vector_store = VectorStoreManager()
    results = vector_store.similarity_search(
        "What is the budget for Project A?",
        filter={"storage_type": "Local"}
    )
    
    print("\nSearch Results:")
    for doc in results:
        print(f"\nSource: {doc.metadata.get('source', 'Unknown')}")
        print(f"Content: {doc.page_content[:200]}...") 