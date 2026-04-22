"""LLM and embedding client factories. Provider specs live in config.py."""

from __future__ import annotations

from typing import Optional, Tuple, Union

import httpx

from config import resolve_llm_config, resolve_embedding_config

TimeoutType = Union[float, Tuple[float, float]]


def _mask_secret(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]} (len={len(value)})"


def log_provider_config() -> None:
    """Print a summary of the resolved LLM + Embedding provider configs."""
    try:
        llm_cfg = resolve_llm_config()
    except Exception as e:
        llm_cfg = {"provider": "?", "base_url": f"<error: {e}>", "api_key": "", "model": "?"}
    try:
        emb_cfg = resolve_embedding_config()
    except Exception as e:
        emb_cfg = {"provider": "?", "base_url": f"<error: {e}>", "api_key": "", "model": "?"}

    lines = [
        "",
        "=" * 60,
        " Provider configuration",
        "=" * 60,
        f"  LLM         : provider={llm_cfg['provider']}",
        f"                model    = {llm_cfg['model']}",
        f"                base_url = {llm_cfg['base_url']}",
        f"                api_key  = {_mask_secret(llm_cfg['api_key'])}",
        f"  Embeddings  : provider={emb_cfg['provider']}",
        f"                model    = {emb_cfg['model']}",
        f"                base_url = {emb_cfg['base_url']}",
        f"                api_key  = {_mask_secret(emb_cfg['api_key'])}",
        "=" * 60,
        "",
    ]
    print("\n".join(lines), flush=True)


def create_llm(
    model: Optional[str] = None,
    streaming: bool = False,
    temperature: float = 0.0,
    timeout: Optional[TimeoutType] = (10.0, 120.0),
    max_retries: int = 6,
):
    from langchain_openai import ChatOpenAI

    cfg = resolve_llm_config(model_override=model)

    kwargs = dict(
        base_url=cfg["base_url"],
        api_key=cfg["api_key"],
        model=cfg["model"],
        temperature=temperature,
        streaming=streaming,
        disable_streaming=not streaming,
        timeout=timeout,
        max_retries=max_retries,
    )
    if cfg["extra_headers"]:
        kwargs["default_headers"] = cfg["extra_headers"]

    return ChatOpenAI(**kwargs)


class OpenAICompatibleEmbeddings:
    """Minimal embeddings client for OpenAI-compatible providers."""

    def __init__(
        self,
        model: str,
        base_url: str,
        api_key: str,
        timeout: TimeoutType = 60.0,
        extra_headers: Optional[dict] = None,
    ):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.extra_headers = extra_headers or {}

    def _embedding_endpoint(self) -> str:
        return f"{self.base_url}/embeddings"

    def _request_embeddings(self, inputs: list[str]) -> list[list[float]]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            **self.extra_headers,
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


class CustomEncodeTextEmbeddings:
    """vLLM-style /encode_text client. One text per request."""

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

    def _endpoint(self) -> str:
        return f"{self.base_url}/encode_text"

    def _extract_vector(self, body) -> list[float]:
        if isinstance(body, list):
            if body and isinstance(body[0], list):
                return body[0]
            return body
        if isinstance(body, dict):
            for key in ("embedding", "vector", "data", "embeddings", "result", "output"):
                value = body.get(key)
                if isinstance(value, list):
                    if value and isinstance(value[0], list):
                        return value[0]
                    if value and isinstance(value[0], dict):
                        inner = value[0].get("embedding") or value[0].get("vector")
                        if isinstance(inner, list):
                            return inner
                    if value and isinstance(value[0], (int, float)):
                        return value
        raise ValueError(
            f"Unrecognized K-Cloud embedding response shape: {str(body)[:500]}"
        )

    def _request_one(self, text: str) -> list[float]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-type": "application/json",
        }
        payload = {"text": text, "model_name": self.model}
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(self._endpoint(), headers=headers, json=payload)
            response.raise_for_status()
            return self._extract_vector(response.json())

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._request_one(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._request_one(text)


def create_embeddings(model: Optional[str] = None):
    cfg = resolve_embedding_config(model_override=model)

    if cfg["client"] == "custom_encode_text":
        return CustomEncodeTextEmbeddings(
            model=cfg["model"],
            base_url=cfg["base_url"],
            api_key=cfg["api_key"],
            timeout=cfg["timeout"],
        )

    return OpenAICompatibleEmbeddings(
        model=cfg["model"],
        base_url=cfg["base_url"],
        api_key=cfg["api_key"],
        timeout=cfg["timeout"],
        extra_headers=cfg["extra_headers"],
    )
