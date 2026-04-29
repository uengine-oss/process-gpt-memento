"""외부 용어집 API 연동 — retrieve 결과에 글로벌 용어 병합용."""
from __future__ import annotations

import os
from typing import List

import httpx
from langchain.schema import Document

ROBO_GLOSSARY_API_BASE_URL = (
    os.getenv("ROBO_GLOSSARY_API_BASE_URL") or "http://127.0.0.1:5504/robo"
).rstrip("/")
ROBO_GLOSSARY_TIMEOUT_SEC = float(os.getenv("ROBO_GLOSSARY_TIMEOUT_SEC", "5"))


async def retrieve_glossary_terms(query: str, tenant_id: str, top_k: int) -> List[Document]:
    normalized_query = (query or "").strip()
    if not normalized_query:
        return []

    search_url = f"{ROBO_GLOSSARY_API_BASE_URL}/glossary/terms/search"
    headers = {
        "Accept": "application/json",
        "X-Tenant-Id": tenant_id,
    }
    params = {
        "query": normalized_query,
        "limit": str(max(1, min(top_k, 20))),
    }

    try:
        async with httpx.AsyncClient(timeout=ROBO_GLOSSARY_TIMEOUT_SEC) as client:
            response = await client.get(search_url, params=params, headers=headers)
            response.raise_for_status()
            payload = response.json() if response.text else {}
    except Exception as e:
        print(f"Glossary retrieval skipped due to error: {e}")
        return []

    docs: List[Document] = []
    for term in payload.get("terms") or []:
        name = str(term.get("name") or "").strip()
        if not name:
            continue
        description = str(term.get("description") or "").strip()
        glossary_name = str(term.get("glossaryName") or "").strip()
        synonyms = term.get("synonyms") if isinstance(term.get("synonyms"), list) else []
        synonyms_text = ", ".join(str(item).strip() for item in synonyms if str(item).strip())

        page_content_lines = [
            f"[용어집] {name}",
            f"설명: {description or '-'}",
        ]
        if glossary_name:
            page_content_lines.append(f"용어집: {glossary_name}")
        if synonyms_text:
            page_content_lines.append(f"동의어: {synonyms_text}")

        docs.append(
            Document(
                page_content="\n".join(page_content_lines),
                metadata={
                    "tenant_id": tenant_id,
                    "knowledge_scope": "global",
                    "source_type": "glossary_term",
                    "file_name": f"glossary-{(glossary_name or 'global').replace(' ', '_')}.txt",
                    "chunk_index": 0,
                    "glossary_id": term.get("glossaryId"),
                    "glossary_name": glossary_name,
                    "term_id": term.get("id"),
                    "term_name": name,
                    "term_status": term.get("status"),
                },
            )
        )
    return docs
