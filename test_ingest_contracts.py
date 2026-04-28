"""Ingest Contract_A and Contract_B (md files) into memento Chroma store.

Uses memento's DocumentProcessor + VectorStoreManager directly so we don't
have to wire Supabase storage just for a test.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from langchain.schema import Document
from document_loader import DocumentProcessor
from vector_store import VectorStoreManager


CONTRACTS = [
    ROOT.parent / "Contract_A_NovaCloud_SaaS_Agreement.md",
    ROOT.parent / "Contract_B_Helios_Cloud_Services_Agreement.md",
]
TENANT_ID = "test-tenant"


async def ingest_one(processor: DocumentProcessor, vs: VectorStoreManager, path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    docs = [Document(page_content=text, metadata={
        "source": path.name,
        "file_name": path.name,
        "file_type": path.suffix.lstrip("."),
        "language": "en",
    })]
    chunks = await processor.process_documents(
        docs, metadata={"file_name": path.name}
    )
    print(f"[{path.name}] -> {len(chunks)} chunks")
    ok = await vs.add_documents(chunks, tenant_id=TENANT_ID)
    print(f"[{path.name}] add_documents ok={ok}")
    return len(chunks)


async def main():
    processor = DocumentProcessor(chunk_size=2000, chunk_overlap=400)
    vs = VectorStoreManager()
    print(f"Chroma-only mode: {vs.chroma_only_mode}")
    print(f"Collection size before: {vs.collection.count()}")
    total = 0
    for p in CONTRACTS:
        if not p.exists():
            print(f"SKIP missing: {p}")
            continue
        total += await ingest_one(processor, vs, p)
    print(f"\nCollection size after: {vs.collection.count()} (added {total})")


if __name__ == "__main__":
    asyncio.run(main())
