"""
Local LLM helper (no external llm_factory dependency).

We intentionally avoid `llm_factory` and build ChatOpenAI directly.
This also hardens against transient streaming/SSE transport errors like:
  httpx.RemoteProtocolError: incomplete chunked read

Key point: LangChain agents may use `.astream()` internally even when `streaming=False`.
Setting `disable_streaming=True` forces the underlying model to not use streaming transport.
"""

from __future__ import annotations

import os
from typing import Optional, Tuple, Union

import httpx

TimeoutType = Union[float, Tuple[float, float]]


def create_llm(
    model: Optional[str] = None,
    streaming: bool = False,  # compatibility + opt-in streaming support
    temperature: float = 0.0,
    timeout: Optional[TimeoutType] = (10.0, 120.0),  # connect, read
    max_retries: int = 6,
):
    """
    Standard ChatOpenAI constructor wrapper used across the project.
    """
    from langchain_openai import ChatOpenAI

    base_url = os.getenv("LLM_PROXY_URL", "http://litellm-proxy:4000")
    api_key = os.getenv("LLM_PROXY_API_KEY") or os.getenv("OPENAI_API_KEY", "")
    resolved_model = (
        model
        or (os.getenv("LLM_MODEL") or "").strip()
        or "gpt-4o"
    )

    # Full compatibility mode:
    # - streaming=False: force non-streaming transport for stability
    # - streaming=True: allow original streaming behavior
    return ChatOpenAI(
        base_url=base_url,
        api_key=api_key,
        model=resolved_model,
        temperature=temperature,
        streaming=streaming,
        disable_streaming=not streaming,
        timeout=timeout,
        max_retries=max_retries,
    )


class OpenAICompatibleEmbeddings:
    """Minimal embeddings client for OpenAI-compatible providers."""

    def __init__(
        self,
        model: str,
        base_url: str,
        api_key: str,
        timeout: TimeoutType = 60.0,
    ):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _embedding_endpoint(self) -> str:
        return f"{self.base_url}/embeddings"

    def _request_embeddings(self, inputs: list[str]) -> list[list[float]]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "input": inputs,
        }

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                self._embedding_endpoint(),
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            body = response.json()

        data = body.get("data")
        if isinstance(data, list):
            embeddings = [
                item.get("embedding")
                for item in data
                if isinstance(item, dict) and isinstance(item.get("embedding"), list)
            ]
            if len(embeddings) == len(inputs):
                return embeddings

        embeddings = body.get("embeddings")
        if isinstance(embeddings, list) and len(embeddings) == len(inputs):
            return embeddings

        raise ValueError(
            "No embedding data received. "
            f"model={self.model}, response_keys={list(body.keys())}, "
            f"response_preview={str(body)[:500]}"
        )

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        return self._request_embeddings(texts)

    def embed_query(self, text: str) -> list[float]:
        return self._request_embeddings([text])[0]


def create_embeddings(model: Optional[str] = None):
    """
    Standard embeddings constructor wrapper used across the project.
    Handles OpenAI-compatible providers whose embeddings responses differ
    slightly from the OpenAI SDK expectations.
    """
    base_url = (
        os.getenv("EMBEDDING_BASE_URL")
        or os.getenv("LLM_PROXY_URL", "http://litellm-proxy:4000")
    )
    api_key = os.getenv("LLM_PROXY_API_KEY") or os.getenv("OPENAI_API_KEY", "")
    resolved_model = (
        model
        or (os.getenv("LLM_EMBEDDING_MODEL") or "").strip()
        or "text-embedding-3-small"
    )
    timeout = float(os.getenv("EMBEDDING_TIMEOUT_SEC", "60"))

    return OpenAICompatibleEmbeddings(
        model=resolved_model,
        base_url=base_url,
        api_key=api_key,
        timeout=timeout,
    )
