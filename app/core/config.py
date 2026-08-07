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
        "supports_vision": True,
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
        "supports_vision": False,
        "api_key_env": ["OPENROUTER_API_KEY", "OPENROUTER_LLM_API_KEY"],
        "base_url_env": ["OPENROUTER_LLM_BASE_URL", "OPENROUTER_BASE_URL"],
        "model_env": ["OPENROUTER_LLM_MODEL"],
    },
    "custom": {
        "base_url": None,
        "model": "/models/openai/gpt-oss-120b",
        "supports_vision": False,
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
        # 사내 GPU 임베딩 서버. bge-m3 (TEI) 가 nginx 30000/v1 로 OpenAI 호환 /v1/embeddings 제공.
        # base_url 은 .../v1 까지 지정 (client 가 {base_url}/embeddings 호출).
        # 구버전엔 K-Cloud /encode_text 서버(client=custom_encode_text)였으나 bge-m3 는
        # OpenAI 호환이라 openai_compatible 로 변경.
        "base_url": None,
        "model": "BAAI/bge-m3",
        "api_key_env": ["CUSTOM_EMBEDDING_API_KEY"],
        "base_url_env": ["CUSTOM_EMBEDDING_BASE_URL"],
        "model_env": ["CUSTOM_EMBEDDING_MODEL"],
        "client": "openai_compatible",
    },
    "self": {
        "base_url": None,
        "model": "Qwen/Qwen3-Embedding-0.6B",
        "api_key_env": [],
        "base_url_env": [],
        "model_env": ["SELF_EMBEDDING_MODEL"],
        "device_env": "SELF_EMBEDDING_DEVICE",
        "client": "self",
    },
}


EMBEDDING_TIMEOUT_SEC: float = 180.0
CHROMA_PERSIST_DIRECTORY: str = "./chroma_db"
CHROMA_COLLECTION_NAME: str = "documents"
VECTOR_BACKEND: str = "chroma"
QDRANT_COLLECTION_NAME: str = "documents"
QDRANT_VECTOR_SIZE: int = 1536
QDRANT_ON_DISK: bool = True
QDRANT_QUANTIZATION: str = "int8"
QDRANT_SEARCH_OVERSAMPLING: float = 2.0
QDRANT_HNSW_EF: int = 100
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
        "supports_vision": bool(spec.get("supports_vision", False)),
        "extra_headers": _openrouter_headers() if provider == "openrouter" else {},
    }


def image_analysis_enabled() -> bool:
    override = os.getenv("MEMENTO_IMAGE_ANALYSIS")
    if override is not None and override.strip() != "":
        return override.strip().lower() in {"1", "true", "yes", "on"}
    return resolve_llm_config().get("supports_vision", False)


def resolve_embedding_config(model_override: Optional[str] = None) -> Dict[str, Any]:
    provider = get_embedding_provider()
    if provider not in EMBEDDING_PROVIDERS:
        raise ValueError(f"Unknown MEMENTO_EMBEDDING_PROVIDER: {provider}")
    spec = EMBEDDING_PROVIDERS[provider]

    base_url = _first_env(spec.get("base_url_env") or []) or spec.get("base_url")
    if provider == "custom" and not base_url:
        raise ValueError("MEMENTO_EMBEDDING_PROVIDER=custom requires CUSTOM_EMBEDDING_BASE_URL")

    cfg: Dict[str, Any] = {
        "provider": provider,
        "base_url": base_url,
        "api_key": _first_env(spec.get("api_key_env") or []),
        "model": model_override or _first_env(spec.get("model_env") or []) or spec["model"],
        "timeout": float(os.getenv("EMBEDDING_TIMEOUT_SEC") or EMBEDDING_TIMEOUT_SEC),
        "client": spec["client"],
        "extra_headers": _openrouter_headers() if provider == "openrouter" else {},
    }

    if provider == "self":
        device_env = spec.get("device_env", "SELF_EMBEDDING_DEVICE")
        cfg["device"] = os.getenv(device_env, "cuda")

    return cfg


def chroma_persist_directory() -> str:
    return _env("CHROMA_PERSIST_DIRECTORY", CHROMA_PERSIST_DIRECTORY)


def chroma_collection_name() -> str:
    return _env("CHROMA_COLLECTION_NAME", CHROMA_COLLECTION_NAME)


def chroma_server_host() -> str:
    """설정 시 Chroma 를 *서버 모드*(HttpClient)로 사용. 빈 값이면 in-process PersistentClient.

    대용량 인덱스에서 인프로세스 Chroma 가 API 프로세스(이벤트 루프)를 얼리는 문제를
    피하려면, Chroma 를 별도 서버로 띄우고 이 값을 그 host 로 지정한다.
    """
    return (_env("CHROMA_SERVER_HOST", "") or "").strip()


def chroma_server_port() -> int:
    try:
        return int(_env("CHROMA_SERVER_PORT", "8000"))
    except (TypeError, ValueError):
        return 8000


def vector_backend() -> str:
    """벡터 인덱스 백엔드: ``chroma`` (기본) 또는 ``qdrant``.

    Chroma 는 HNSW·원본 벡터를 전부 RAM 에 상주시켜 대용량(수백만 벡터)에서 메모리로
    죽는다. Qdrant 백엔드는 int8 양자화본만 RAM 에 두고 원본은 디스크에 두는 구성이라
    상주 메모리가 1/4 로 줄어든다. 전환은 이 값 하나로 한다.
    """
    return (_env("VECTOR_BACKEND", VECTOR_BACKEND) or VECTOR_BACKEND).strip().lower()


def qdrant_url() -> str:
    """Qdrant REST endpoint. 미설정이면 host/port 로 조립."""
    explicit = (_env("QDRANT_URL", "") or "").strip()
    if explicit:
        return explicit.rstrip("/")
    host = (_env("QDRANT_HOST", "127.0.0.1") or "127.0.0.1").strip()
    try:
        port = int(_env("QDRANT_PORT", "6333"))
    except (TypeError, ValueError):
        port = 6333
    return f"http://{host}:{port}"


def qdrant_api_key() -> Optional[str]:
    return (_env("QDRANT_API_KEY", "") or "").strip() or None


def qdrant_collection_name() -> str:
    # 기본값을 Chroma 컬렉션명과 맞춰, 이관 후에도 같은 이름으로 읽힌다.
    return _env("QDRANT_COLLECTION_NAME", QDRANT_COLLECTION_NAME)


def qdrant_vector_size() -> int:
    """컬렉션 생성 시에만 쓰인다. 기존 컬렉션이 있으면 그쪽 설정이 우선."""
    try:
        return int(_env("QDRANT_VECTOR_SIZE", str(QDRANT_VECTOR_SIZE)))
    except (TypeError, ValueError):
        return QDRANT_VECTOR_SIZE


def qdrant_on_disk() -> bool:
    """원본 벡터/HNSW/payload 를 디스크(mmap)에 둘지. 메모리 절감의 핵심 스위치."""
    raw = _env("QDRANT_ON_DISK", "")
    if not raw.strip():
        return QDRANT_ON_DISK
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def qdrant_quantization() -> str:
    """``int8`` (기본) | ``binary`` | ``none``.

    int8: 벡터당 1/4 크기, 정확도 손실 미미 — 기본값.
    binary: 1/32 크기. 1536-dim 고차원에서만 쓸 만하고 oversampling 을 크게 줘야 한다.
    none: 양자화 없음 — 원본이 그대로 RAM/디스크에서 쓰인다.
    """
    return (_env("QDRANT_QUANTIZATION", QDRANT_QUANTIZATION) or "int8").strip().lower()


def qdrant_search_oversampling() -> float:
    """양자화 검색 시 후보를 몇 배로 넓게 뽑아 원본으로 재채점할지."""
    try:
        return float(_env("QDRANT_SEARCH_OVERSAMPLING", str(QDRANT_SEARCH_OVERSAMPLING)))
    except (TypeError, ValueError):
        return QDRANT_SEARCH_OVERSAMPLING


def qdrant_hnsw_ef() -> int:
    """검색 시 탐색 폭. Chroma 쪽 ef_search=100 과 맞춰 리콜을 보존한다."""
    try:
        return int(_env("QDRANT_HNSW_EF", str(QDRANT_HNSW_EF)))
    except (TypeError, ValueError):
        return QDRANT_HNSW_EF


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


def supabase_public_url() -> str:
    """브라우저(외부)에서 접근 가능한 Supabase base URL.

    내부망 배포 시 ``SUPABASE_URL`` 은 ``http://kong:8000`` 같은 컨테이너 내부
    hostname 이라 브라우저가 접근 못 한다. 외부 노출용 URL 을 ``SUPABASE_PUBLIC_URL``
    (또는 ``API_EXTERNAL_URL``) 로 지정한다.
    """
    return (os.getenv("SUPABASE_PUBLIC_URL", "") or os.getenv("API_EXTERNAL_URL", "") or "").strip()


def rewrite_storage_public_host(url: str) -> str:
    """Storage URL 의 내부 host(``SUPABASE_URL``) 를 외부 접근용 host 로 교체.

    내부망 배포에서 supabase 클라이언트가 만든 signed/public URL 은
    ``http://kong:8000/storage/v1/...`` 처럼 컨테이너 내부 hostname 을 박는다.
    이 URL 을 그대로 브라우저로 내려보내면 다운로드가 안 되므로
    ``SUPABASE_PUBLIC_URL`` (또는 ``API_EXTERNAL_URL``) prefix 로 교체한다.

    예:
      INTERNAL: http://kong:8000/storage/v1/object/sign/files/...
      PUBLIC:   http://<hostip>:8088/storage/v1/object/sign/files/...

    내부/외부 URL 중 한 쪽이 비었거나 같으면 (= 공개 도메인 운영) 원본 그대로 반환.
    """
    if not url:
        return url
    internal = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
    public = supabase_public_url().rstrip("/")
    if not internal or not public or internal == public:
        return url
    if url.startswith(internal):
        return public + url[len(internal):]
    return url
