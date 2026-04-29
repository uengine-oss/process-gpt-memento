"""ChromaDB 청크 조회 스크립트

사용법:
  python inspect_chroma.py                    # 전체 요약 + 최근 5개 청크
  python inspect_chroma.py --all              # 전체 청크 출력
  python inspect_chroma.py --room ROOM_ID     # 특정 채팅방 청크만
  python inspect_chroma.py --file FILE_NAME   # 특정 파일 청크만
  python inspect_chroma.py --search "검색어"  # 유사도 검색
"""

import argparse
import sys
from chromadb import PersistentClient


def get_collection():
    from app.core from app.core import config
    client = PersistentClient(path=config.chroma_persist_directory())
    return client.get_collection(config.chroma_collection_name().strip())


def print_chunk(i, doc_id, doc, meta):
    file_name = meta.get("file_name", "?")
    chunk_idx = meta.get("chunk_index", "?")
    room_id = meta.get("room_id", "-")
    tenant_id = meta.get("tenant_id", "-")
    section = meta.get("section_title", "")
    page = meta.get("page_number", "?")

    print(f"--- 청크 {i} ---")
    print(f"  ID:       {doc_id}")
    print(f"  파일:     {file_name}  (청크 {chunk_idx}, 페이지 {page})")
    print(f"  섹션:     {section}")
    print(f"  room_id:  {room_id}")
    print(f"  tenant:   {tenant_id}")
    print(f"  내용({len(doc)}자):")
    preview = doc[:300] if len(doc) > 300 else doc
    for line in preview.splitlines():
        print(f"    {line}")
    if len(doc) > 300:
        print(f"    ... ({len(doc) - 300}자 생략)")
    print()


def cmd_list(col, where, limit):
    kwargs = {"include": ["documents", "metadatas"]}
    if where:
        kwargs["where"] = where
    if limit:
        kwargs["limit"] = limit
    result = col.get(**kwargs)

    ids = result.get("ids", [])
    docs = result.get("documents", [])
    metas = result.get("metadatas", [])

    if not ids:
        print("조회된 청크가 없습니다.")
        return

    for i, (doc_id, doc, meta) in enumerate(zip(ids, docs, metas), 1):
        print_chunk(i, doc_id, doc, meta)


def cmd_search(col, query, top_k, where):
    from app.services.llm import create_embeddings
    emb = create_embeddings()
    query_vec = emb.embed_query(query)

    kwargs = {
        "query_embeddings": [query_vec],
        "n_results": top_k,
        "include": ["documents", "metadatas", "distances"],
    }
    if where:
        kwargs["where"] = where

    result = col.query(**kwargs)
    ids = result.get("ids", [[]])[0]
    docs = result.get("documents", [[]])[0]
    metas = result.get("metadatas", [[]])[0]
    distances = result.get("distances", [[]])[0]

    if not ids:
        print("검색 결과가 없습니다.")
        return

    print(f'검색어: "{query}"  (top {top_k})\n')
    for i, (doc_id, doc, meta, dist) in enumerate(zip(ids, docs, metas, distances), 1):
        print(f"[유사도 거리: {dist:.4f}]")
        print_chunk(i, doc_id, doc, meta)


def main():
    parser = argparse.ArgumentParser(description="ChromaDB 청크 조회")
    parser.add_argument("--all", action="store_true", help="전체 청크 출력")
    parser.add_argument("--room", type=str, help="room_id로 필터링")
    parser.add_argument("--file", type=str, help="file_name으로 필터링")
    parser.add_argument("--tenant", type=str, help="tenant_id로 필터링")
    parser.add_argument("--search", type=str, help="유사도 검색")
    parser.add_argument("--top", type=int, default=5, help="검색 결과 개수 (기본 5)")
    parser.add_argument("--limit", type=int, default=None, help="조회 개수 제한")
    args = parser.parse_args()

    col = get_collection()
    total = col.count()
    print(f"컬렉션: {col.name}  |  총 청크 수: {total}\n")

    if total == 0:
        return

    # where 필터 구성
    conditions = []
    if args.room:
        conditions.append({"room_id": args.room})
    if args.file:
        conditions.append({"file_name": args.file})
    if args.tenant:
        conditions.append({"tenant_id": args.tenant})

    where = None
    if len(conditions) == 1:
        where = conditions[0]
    elif len(conditions) > 1:
        where = {"$and": conditions}

    if args.search:
        cmd_search(col, args.search, args.top, where)
    else:
        limit = None if args.all else (args.limit or 5)
        cmd_list(col, where, limit)


if __name__ == "__main__":
    main()
