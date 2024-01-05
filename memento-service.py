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

from llama_index.node_parser import SimpleNodeParser

from typing import Union
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse

app = FastAPI()

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))


PERSIST_DIR = "./storage"
global index
if not os.path.exists(PERSIST_DIR):
    documents = SimpleDirectoryReader("data").load_data()
    index = VectorStoreIndex.from_documents(documents)
    # store it for later
    index.storage_context.persist(persist_dir=PERSIST_DIR)
else:
    # load the existing index
    storage_context = StorageContext.from_defaults(persist_dir=PERSIST_DIR)
    index = load_index_from_storage(storage_context)

global query_engine
query_engine = index.as_query_engine()


@app.get("/query")
def read_item(query: str):
    response = query_engine.query(query)

    print(type(response))
    print(response.metadata)
    print(response)

    return response


import os
import shutil

drop_directory = "./drop"

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    with open(f"{drop_directory}/{file.filename}", "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"filename": file.filename}

@app.get("/")
async def main():
    content = """
<form action="/uploadfile/" enctype="multipart/form-data" method="post">
<input name="file" type="file">
<input type="submit">
</form>
"""
    return HTMLResponse(content="<script>window.location='/query'</script>")
    

# @app.get("/queryform")
# async def queryform():
#     content = """
# <form action="/query/" method="get">
# <input name="query" type="text">
# <input type="submit">
# </form>
# """
#     return HTMLResponse(content=content)


# ---- intervally search directory and indexing ----
from threading import Thread
import time

def find_first_file(directory):
    for file in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, file)):
            return file
    return None

def delete_all_files_in_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def index_periodically(directory, interval):
    global query_engine
    while True:
        file_name = find_first_file(directory)
        if file_name:
            print(file_name)

            documents = SimpleDirectoryReader(directory).load_data()
            # new_index = VectorStoreIndex.from_documents(documents)
            #     # store it for later
            # new_index.storage_context.persist(persist_dir=PERSIST_DIR)
                    
            parser = SimpleNodeParser()
            new_nodes = parser.get_nodes_from_documents(documents)

            # Add nodes to the existing index
            print("Adding new nodes to the existing index...")
            index.insert_nodes(new_nodes)
            index.storage_context.persist(persist_dir=PERSIST_DIR)
            query_engine = index.as_query_engine()

            delete_all_files_in_directory(directory)
            print("Indexing Done.")

        # else:
        #     print(".")
        time.sleep(interval)

@app.on_event("startup")
def startup_event():

    interval = 10
    thread = Thread(target=index_periodically, args=(drop_directory, interval))
    thread.start()