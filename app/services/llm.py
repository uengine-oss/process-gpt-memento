"""LLM and embedding client factories. Provider specs live in config.py."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple, Union

import httpx

from app.core.config import resolve_llm_config, resolve_chat_params, resolve_embedding_config
from app.services.llm_output import message_text

logger = logging.getLogger(__name__)

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


# ChatOpenAI 가 1급 인자로 받는 것과 vLLM 확장(extra_body 로 가야 하는 것)의 경계.
_FIRST_CLASS_SAMPLING = ("top_p", "presence_penalty", "frequency_penalty")


def _resolved_params(
    cfg: Dict[str, Any],
    temperature: Optional[float],
    max_tokens: Optional[int] = None,
) -> Dict[str, Dict[str, Any]]:
    return resolve_chat_params(
        provider=cfg["provider"],
        model=cfg["model"],
        temperature=temperature,
        max_tokens=max_tokens,
    )


def create_llm(
    model: Optional[str] = None,
    streaming: bool = False,
    temperature: Optional[float] = None,
    timeout: Optional[TimeoutType] = (10.0, 120.0),
    max_retries: int = 6,
):
    """설정된 프로바이더의 LLM 클라이언트.

    무엇을 실어 보낼지는 ``config/llm_sampling.json`` 이 정한다. 호출부의
    ``temperature`` 는 모델이 그 값을 허용할 때만 이긴다.
    """
    from langchain_openai import ChatOpenAI

    cfg = resolve_llm_config(model_override=model)
    resolved = _resolved_params(cfg, temperature)
    sampling = dict(resolved["sampling"])
    extra_body = dict(resolved["extra_body"])

    kwargs: Dict[str, Any] = dict(
        base_url=cfg["base_url"],
        api_key=cfg["api_key"],
        model=cfg["model"],
        streaming=streaming,
        disable_streaming=not streaming,
        timeout=timeout,
        max_retries=max_retries,
    )
    # langchain 은 temperature 를 늘 싣는다(필드 기본 0.7). 모델이 값을 강제하면
    # 그 값으로 덮어야 하고, 아무 설정이 없으면 결정론을 위해 0.0 이다.
    kwargs["temperature"] = sampling.pop("temperature", 0.0)
    for name in _FIRST_CLASS_SAMPLING:
        if name in sampling:
            kwargs[name] = sampling.pop(name)
    # 남은 값은 프로바이더 확장이므로 요청 본문에 그대로 실어 보낸다.
    # (langchain_openai 는 extra_body 를 model_kwargs 안에 숨기면 거부한다.)
    extra_body.update(sampling)
    if extra_body:
        kwargs["extra_body"] = extra_body
    if cfg["extra_headers"]:
        kwargs["default_headers"] = cfg["extra_headers"]

    return ChatOpenAI(**kwargs)


def chat_completion(
    *,
    messages: list,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: TimeoutType = 300.0,
    retries: int = 1,
    extra_payload: Optional[Dict[str, Any]] = None,
    log_prefix: str = "llm",
) -> str:
    """OpenAI 호환 ``/chat/completions`` 직접 호출. 실패하면 빈 문자열.

    langchain 을 거치지 않는 호출부(vision·구조화기)가 쓰는 단일 경로다. 모델별
    파라미터를 여기서 한 번만 맞추므로 호출부는 messages 만 만들면 된다.
    """
    cfg = resolve_llm_config()
    base_url = (cfg.get("base_url") or "").rstrip("/")
    model = cfg.get("model") or ""
    if not base_url or not model:
        logger.warning("[%s] base_url/model 미설정 - skip", log_prefix)
        return ""

    resolved = _resolved_params(cfg, temperature, max_tokens)
    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        **resolved["sampling"],
        **resolved["extra_body"],
        **(extra_payload or {}),
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cfg.get('api_key') or 'not-needed'}",
        **(cfg.get("extra_headers") or {}),
    }

    last_error = ""
    for _ in range(max(1, retries)):
        try:
            with httpx.Client(timeout=timeout) as client:
                response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
            if response.status_code >= 400:
                # 본문을 삼키면 무엇이 거부됐는지 못 본다.
                last_error = f"{response.status_code} {response.text[:300]}"
                continue
            return message_text(response.json())
        except Exception as exc:  # noqa: BLE001 - 호출 실패가 파싱 전체를 막지 않는다
            last_error = str(exc)
    logger.warning("[%s] 호출 실패: %s", log_prefix, last_error)
    return ""


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
            # litellm 경유 시 litellm이 기본 base64를 주입해 openrouter가 거부(400)하므로
            # 클라이언트가 명시적으로 float를 요청한다. OpenAI/openrouter 모두 float 지원.
            "encoding_format": "float",
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
