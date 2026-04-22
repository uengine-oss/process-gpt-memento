"""Static configuration. env holds secrets and per-env switches only."""
from __future__ import annotations

import os
from typing import Any, Dict, Optional


def _env(name: str, default: Any = None) -> Any:
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else v


LLM_PROVIDERS: Dict[str, Dict[str, Any]] = {
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o",
        "api_key_env": [
            "OPENAI_LLM_API_KEY",
            "LLM_API_KEY",
            "LLM_PROXY_API_KEY",
            "OPENAI_API_KEY",
        ],
        "base_url_env": ["OPENAI_LLM_BASE_URL", "LLM_BASE_URL", "LLM_PROXY_URL"],
        "model_env": ["OPENAI_LLM_MODEL", "LLM_MODEL"],
    },
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "model": "openai/gpt-oss-120b",
        "api_key_env": ["OPENROUTER_API_KEY", "OPENROUTER_LLM_API_KEY"],
        "base_url_env": ["OPENROUTER_LLM_BASE_URL", "OPENROUTER_BASE_URL"],
        "model_env": ["OPENROUTER_LLM_MODEL"],
    },
    "custom": {
        "base_url": None,
        "model": "/models/openai/gpt-oss-120b",
        "api_key_env": ["CUSTOM_LLM_API_KEY"],
        "base_url_env": ["CUSTOM_LLM_BASE_URL"],
        "model_env": ["CUSTOM_LLM_MODEL"],
    },
}


EMBEDDING_PROVIDERS: Dict[str, Dict[str, Any]] = {
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "text-embedding-3-small",
        "api_key_env": [
            "OPENAI_EMBEDDING_API_KEY",
            "EMBEDDING_API_KEY",
            "LLM_PROXY_API_KEY",
            "OPENAI_API_KEY",
        ],
        "base_url_env": [
            "OPENAI_EMBEDDING_BASE_URL",
            "EMBEDDING_BASE_URL",
            "LLM_PROXY_URL",
        ],
        "model_env": ["OPENAI_EMBEDDING_MODEL", "LLM_EMBEDDING_MODEL"],
        "client": "openai_compatible",
    },
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "model": "qwen/qwen3-embedding-4b",
        "api_key_env": ["OPENROUTER_API_KEY", "OPENROUTER_EMBEDDING_API_KEY"],
        "base_url_env": ["OPENROUTER_EMBEDDING_BASE_URL", "OPENROUTER_BASE_URL"],
        "model_env": ["OPENROUTER_EMBEDDING_MODEL"],
        "client": "openai_compatible",
    },
    "custom": {
        "base_url": None,
        "model": "Qwen3-Embedding-4B",
        "api_key_env": ["CUSTOM_EMBEDDING_API_KEY"],
        "base_url_env": ["CUSTOM_EMBEDDING_BASE_URL"],
        "model_env": ["CUSTOM_EMBEDDING_MODEL"],
        "client": "custom_encode_text",
    },
}


EMBEDDING_TIMEOUT_SEC: float = 180.0
CHROMA_PERSIST_DIRECTORY: str = "./chroma_db"
CHROMA_COLLECTION_NAME: str = "documents"
SUPABASE_WRITE_EMBEDDING: bool = False
SUPABASE_DUMMY_EMBEDDING_DIMENSIONS: int = 1536
OPENROUTER_HTTP_REFERER: Optional[str] = None
OPENROUTER_APP_TITLE: Optional[str] = None
MEMENTO_DRIVE_FOLDER_ID: str = "1jKXip_MCDJFO7sXrvqhGD_i45_7wdp-v"


def _first_env(names: list[str]) -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip() != "":
            return v
    return ""


def get_llm_provider() -> str:
    return (os.getenv("MEMENTO_LLM_PROVIDER") or "openai").strip().lower()


def get_embedding_provider() -> str:
    return (os.getenv("MEMENTO_EMBEDDING_PROVIDER") or "openai").strip().lower()


def _openrouter_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {}
    referer = os.getenv("OPENROUTER_HTTP_REFERER") or OPENROUTER_HTTP_REFERER
    title = os.getenv("OPENROUTER_APP_TITLE") or OPENROUTER_APP_TITLE
    if referer:
        headers["HTTP-Referer"] = referer
    if title:
        headers["X-Title"] = title
    return headers


def resolve_llm_config(model_override: Optional[str] = None) -> Dict[str, Any]:
    provider = get_llm_provider()
    if provider not in LLM_PROVIDERS:
        raise ValueError(f"Unknown MEMENTO_LLM_PROVIDER: {provider}")
    spec = LLM_PROVIDERS[provider]

    base_url = _first_env(spec["base_url_env"]) or spec["base_url"]
    if provider == "custom" and not base_url:
        raise ValueError("MEMENTO_LLM_PROVIDER=custom requires CUSTOM_LLM_BASE_URL")

    return {
        "provider": provider,
        "base_url": base_url,
        "api_key": _first_env(spec["api_key_env"]),
        "model": model_override or _first_env(spec["model_env"]) or spec["model"],
        "extra_headers": _openrouter_headers() if provider == "openrouter" else {},
    }


def resolve_embedding_config(model_override: Optional[str] = None) -> Dict[str, Any]:
    provider = get_embedding_provider()
    if provider not in EMBEDDING_PROVIDERS:
        raise ValueError(f"Unknown MEMENTO_EMBEDDING_PROVIDER: {provider}")
    spec = EMBEDDING_PROVIDERS[provider]

    base_url = _first_env(spec["base_url_env"]) or spec["base_url"]
    if provider == "custom" and not base_url:
        raise ValueError("MEMENTO_EMBEDDING_PROVIDER=custom requires CUSTOM_EMBEDDING_BASE_URL")

    return {
        "provider": provider,
        "base_url": base_url,
        "api_key": _first_env(spec["api_key_env"]),
        "model": model_override or _first_env(spec["model_env"]) or spec["model"],
        "timeout": float(os.getenv("EMBEDDING_TIMEOUT_SEC") or EMBEDDING_TIMEOUT_SEC),
        "client": spec["client"],
        "extra_headers": _openrouter_headers() if provider == "openrouter" else {},
    }


def chroma_persist_directory() -> str:
    return _env("CHROMA_PERSIST_DIRECTORY", CHROMA_PERSIST_DIRECTORY)


def chroma_collection_name() -> str:
    return _env("CHROMA_COLLECTION_NAME", CHROMA_COLLECTION_NAME)


def supabase_write_embedding() -> bool:
    raw = os.getenv("SUPABASE_WRITE_EMBEDDING")
    if raw is None or raw.strip() == "":
        return SUPABASE_WRITE_EMBEDDING
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def supabase_dummy_embedding_dimensions() -> int:
    raw = os.getenv("SUPABASE_DUMMY_EMBEDDING_DIMENSIONS")
    if raw is None or raw.strip() == "":
        return SUPABASE_DUMMY_EMBEDDING_DIMENSIONS
    try:
        return int(raw)
    except ValueError:
        return SUPABASE_DUMMY_EMBEDDING_DIMENSIONS


def memento_drive_folder_id() -> str:
    return _env("MEMENTO_DRIVE_FOLDER_ID", MEMENTO_DRIVE_FOLDER_ID)
