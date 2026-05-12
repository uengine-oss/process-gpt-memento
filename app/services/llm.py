"""LLM and embedding client factories. Provider specs live in config.py."""

from __future__ import annotations

from typing import Optional, Tuple, Union

import httpx

from app.core.config import resolve_llm_config, resolve_embedding_config

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


class SelfHostedEmbeddings:
    """로컬 GPU/CPU에서 sentence-transformers 모델을 직접 실행하는 임베딩 클라이언트.

    Qwen3-Embedding 계열 모델을 지원하며, query 인코딩 시 모델 권장 instruction
    prefix를 자동으로 적용한다.
    """

    _QWEN3_QUERY_PROMPT = (
        "Instruct: Given a query, retrieve relevant passages that answer the query\nQuery: "
    )

    def __init__(self, model: str, device: str = "cuda"):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise ImportError(
                "sentence-transformers 패키지가 필요합니다: pip install sentence-transformers"
            ) from exc

        import torch

        print(f"[SelfHostedEmbeddings] 모델 로딩: {model} (device={device}, dtype=float16)")
        # FP16으로 로딩: 모델 VRAM 절반 절감 + 추론 속도 향상
        model_kwargs = {"torch_dtype": torch.float16} if device == "cuda" else {}
        self._model = SentenceTransformer(model, device=device, model_kwargs=model_kwargs)
        self._device = device
        self._model_name = model
        self._is_qwen3 = "qwen3" in model.lower()
        print(f"[SelfHostedEmbeddings] 모델 로딩 완료")

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        embeddings = self._model.encode(
            texts,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return embeddings.tolist()

    def embed_query(self, text: str) -> list[float]:
        # Qwen3-Embedding 모델은 query 인코딩 시 instruction prefix 권장
        if self._is_qwen3:
            prompt_texts = [self._QWEN3_QUERY_PROMPT + text]
        else:
            prompt_texts = [text]

        embedding = self._model.encode(
            prompt_texts,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return embedding[0].tolist()


import threading as _threading

_embeddings_instance = None
_embeddings_lock = _threading.Lock()


def _new_embeddings_instance(model: Optional[str] = None):
    """Provider 설정에 따라 새 임베딩 인스턴스를 생성하는 내부 팩토리."""
    cfg = resolve_embedding_config(model_override=model)

    if cfg["client"] == "self":
        return SelfHostedEmbeddings(
            model=cfg["model"],
            device=cfg.get("device", "cuda"),
        )

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


def get_embeddings(model: Optional[str] = None):
    """임베딩 인스턴스 싱글턴 getter.

    provider 종류(self/custom/openai)에 관계없이 프로세스 전체에서 동일한
    인스턴스를 반환한다. 처음 호출 시에만 모델/클라이언트를 초기화한다.
    """
    global _embeddings_instance
    if _embeddings_instance is None:
        with _embeddings_lock:
            if _embeddings_instance is None:
                _embeddings_instance = _new_embeddings_instance(model)
    return _embeddings_instance


# 하위 호환: 기존 코드가 create_embeddings()를 직접 import해 쓰는 곳을 위한 alias.
create_embeddings = get_embeddings
