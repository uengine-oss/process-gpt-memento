"""
Retrieval 전략 레지스트리.

벡터 검색은 에이전트가 진입점을 잡는 *힌트* 용도라 쿼리를 그대로 임베딩하는 ``plain``
하나만 둔다. 패러프레이즈·HyDE 같은 쿼리 확장은 호출측 에이전트가 자기 질문을 고쳐
가며 다시 검색하는 일이라 서버에 두지 않는다.

알 수 없는 전략명이 들어오면 ``plain`` 으로 폴백한다.
"""
from typing import Dict, Type

from . import config
from .base import BaseRetriever
from .plain import PlainRetriever


_REGISTRY: Dict[str, Type[BaseRetriever]] = {
    PlainRetriever.name: PlainRetriever,
}


def available_strategies() -> list[str]:
    return list(_REGISTRY.keys())


def get_retriever(strategy: str | None = None, top_k: int | None = None) -> BaseRetriever:
    """Retriever 인스턴스 팩토리. top_k 는 호출마다 retrieve() 인자로 다시 넘어온다."""
    name = (strategy or config.STRATEGY or "plain").strip().lower()
    cls = _REGISTRY.get(name)
    if cls is None:
        print(f"[retrievers] 알 수 없는 전략 '{name}' → 'plain'으로 폴백")
        cls = PlainRetriever
    kwargs: dict = {}
    if top_k is not None:
        kwargs["top_k"] = top_k
    return cls(**kwargs)


def log_active_strategy() -> None:
    """서버 시작 시 현재 선택된 검색 전략을 로깅한다."""
    name = (config.STRATEGY or "plain").strip().lower()
    if name not in _REGISTRY:
        name = "plain"
    print(
        "\n".join([
            "",
            "=" * 60,
            " Retrieval configuration",
            "=" * 60,
            f"  strategy   : {name}",
            f"  available  : {', '.join(_REGISTRY.keys())}",
            "=" * 60,
            "",
        ]),
        flush=True,
    )


__all__ = [
    "BaseRetriever",
    "PlainRetriever",
    "get_retriever",
    "available_strategies",
    "log_active_strategy",
]
