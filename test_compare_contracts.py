"""End-to-end agent-style test:
  1. For each file, call memento /retrieve with file_name filter to get
     security-clause chunks unique to that file.
  2. Concatenate the per-file contexts into a single LLM prompt.
  3. Ask the frentis-ai LLM to compare the two contracts on a chosen topic.

This emulates what a base-agent tool would do server-side.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import List, Dict, Any

import httpx

MEMENTO_URL = os.getenv("MEMENTO_URL", "http://127.0.0.1:8005")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://ai-server.dream-flow.com:30000/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "frentis")
LLM_MODEL = os.getenv("LLM_MODEL", "frentis-ai-model")
TENANT_ID = "test-tenant"

FILE_A = "Contract_A_NovaCloud_SaaS_Agreement.md"
FILE_B = "Contract_B_Helios_Cloud_Services_Agreement.md"
QUERY = "data protection and security incident notification obligations"
TOP_K = 5


async def retrieve(client: httpx.AsyncClient, *, file_name: str) -> List[Dict[str, Any]]:
    r = await client.get(
        f"{MEMENTO_URL}/retrieve",
        params={
            "query": QUERY,
            "tenant_id": TENANT_ID,
            "all_docs": True,
            "top_k": TOP_K,
            "file_name": file_name,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("response", [])


def fmt_chunks(label: str, docs: List[Dict[str, Any]]) -> str:
    lines = [f"### {label}"]
    for d in docs:
        meta = d.get("metadata") or {}
        ci = meta.get("chunk_index")
        lines.append(f"\n--- chunk #{ci} ---")
        lines.append(d.get("page_content", "").strip())
    return "\n".join(lines)


async def call_llm(prompt: str) -> str:
    async with httpx.AsyncClient(timeout=180) as client:
        r = await client.post(
            f"{LLM_BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            json={
                "model": LLM_MODEL,
                "temperature": 0.0,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a contract analyst. Compare the two contracts "
                            "ONLY using the provided excerpts. Quote clause numbers."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
            },
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]


async def main():
    async with httpx.AsyncClient() as client:
        print(f"[1/3] Retrieving from {FILE_A} ...")
        docs_a = await retrieve(client, file_name=FILE_A)
        print(f"      -> {len(docs_a)} chunks")
        print(f"[2/3] Retrieving from {FILE_B} ...")
        docs_b = await retrieve(client, file_name=FILE_B)
        print(f"      -> {len(docs_b)} chunks")

    # Sanity: all chunks must belong to their file
    for d in docs_a:
        assert (d.get("metadata") or {}).get("file_name") == FILE_A, (
            f"Filter leak: {d['metadata']}"
        )
    for d in docs_b:
        assert (d.get("metadata") or {}).get("file_name") == FILE_B, (
            f"Filter leak: {d['metadata']}"
        )

    context_a = fmt_chunks(f"Contract A — {FILE_A}", docs_a)
    context_b = fmt_chunks(f"Contract B — {FILE_B}", docs_b)

    user_prompt = (
        f"Topic: {QUERY}\n\n"
        "Compare Contract A and Contract B on this topic. "
        "For each contract, list the relevant clauses with their numbers, "
        "then produce a side-by-side difference table.\n\n"
        f"{context_a}\n\n{context_b}"
    )

    print(f"[3/3] Calling LLM ({LLM_MODEL}) ...")
    answer = await call_llm(user_prompt)
    print("\n" + "=" * 70)
    print("LLM RESPONSE")
    print("=" * 70)
    print(answer)


if __name__ == "__main__":
    asyncio.run(main())
