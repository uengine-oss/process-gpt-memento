from llama_index import VectorStoreIndex, SimpleDirectoryReader
import logging
import sys
import os.path
from llama_index import (
    VectorStoreIndex,
    SimpleDirectoryReader,
    StorageContext,
    load_index_from_storage,
)

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))


from pathlib import Path
from llama_index import download_loader

PptxReader = download_loader("PptxReader")



PERSIST_DIR = "./storage"
if not os.path.exists(PERSIST_DIR):
    # load the documents and create the index
    loader = PptxReader()
    documents = loader.load_data(file=Path('./doc.pptx'))

    index = VectorStoreIndex.from_documents(documents)
    # store it for later
    index.storage_context.persist(persist_dir=PERSIST_DIR)
else:
    # load the existing index
    storage_context = StorageContext.from_defaults(persist_dir=PERSIST_DIR)
    index = load_index_from_storage(storage_context)


query_engine = index.as_query_engine()
response = query_engine.query("무른모라는 회사의 소프트웨어를 사용하는 사용자수는 얼마야?")
print(response)


