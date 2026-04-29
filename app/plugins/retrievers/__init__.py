"""
Retrieval 전략 레지스트리.

활성 전략과 파라미터는 `retrievers/config.py`에서 설정합니다.
해당 파일을 수정한 뒤 서비스를 재시작하면 전략이 교체됩니다.

사용 가능한 전략:
  - plain        (기본값, 쿼리 그대로 검색. LLM 호출 없음)
  - multi_query  (LLM이 쿼리 N개 패러프레이즈 → 병합)
  - hyde         (LLM이 가상 답변 생성 → 그걸로 검색)
  - rag_fusion   (multi_query + RRF 재순위)
  - rewrite      (LLM이 검색 친화적 쿼리로 재작성)

알 수 없는 전략명이 들어오면 `plain`으로 자동 폴백한다.
"""
from typing import Dict, Type

from . import config
from .base import BaseRetriever
from .plain import PlainRetriever
from .multi_query import MultiQueryRetriever
from .hyde import HyDERetriever
from .rag_fusion import RAGFusionRetriever
from .rewrite import RewriteRetriever


_REGISTRY: Dict[str, Type[BaseRetriever]] = {
    PlainRetriever.name: PlainRetriever,
    MultiQueryRetriever.name: MultiQueryRetriever,
    HyDERetriever.name: HyDERetriever,
    RAGFusionRetriever.name: RAGFusionRetriever,
    RewriteRetriever.name: RewriteRetriever,
}


def available_strategies() -> list[str]:
    return list(_REGISTRY.keys())


def get_retriever(strategy: str | None = None, top_k: int | None = None) -> BaseRetriever:
    """
    Retriever 인스턴스 팩토리.

    top_k는 /retrieve 엔드포인트에서 호출마다 전달되므로 여기서는 생성자에
    기본값으로만 박아둔다. 실제 검색 시 retriever.retrieve(..., top_k=...)가
    호출되면 그 값이 최종적으로 사용된다.
    """
    name = (strategy or config.STRATEGY or "plain").strip().lower()
    cls = _REGISTRY.get(name)
    if cls is None:
        print(f"[retrievers] 알 수 없는 전략 '{name}' → 'plain'으로 폴백")
        cls = PlainRetriever
        name = "plain"

    per_strategy = config.PER_STRATEGY_OVERRIDES.get(name, {}) or {}

    kwargs: dict = {}
    if top_k is not None:
        kwargs["top_k"] = top_k

    if cls is MultiQueryRetriever:
        kwargs["query_count"] = per_strategy.get("query_count", config.MULTI_QUERY_COUNT)
    elif cls is RAGFusionRetriever:
        kwargs["query_count"] = per_strategy.get("query_count", config.MULTI_QUERY_COUNT)
        kwargs["rrf_k"] = per_strategy.get("rrf_k", config.RAG_FUSION_RRF_K)
    elif cls is HyDERetriever:
        kwargs["doc_length"] = per_strategy.get("doc_length", config.HYDE_DOC_LENGTH)
        kwargs["include_original_query"] = per_strategy.get(
            "include_original_query", config.HYDE_INCLUDE_ORIGINAL_QUERY
        )

    print(f"[retrievers] '{name}' 전략 사용")
    return cls(**kwargs)


def log_active_strategy() -> None:
    """서버 시작 시 현재 선택된 검색 전략을 로깅한다."""
    name = (config.STRATEGY or "plain").strip().lower()
    if name not in _REGISTRY:
        name = "plain"

    per_strategy = config.PER_STRATEGY_OVERRIDES.get(name, {}) or {}

    lines = [
        "",
        "=" * 60,
        " Retrieval configuration",
        "=" * 60,
        f"  strategy   : {name}",
    ]
    if name in ("multi_query", "rag_fusion"):
        qc = per_strategy.get("query_count", config.MULTI_QUERY_COUNT)
        lines.append(f"  query_count: {qc}")
    if name == "rag_fusion":
        rrf = per_strategy.get("rrf_k", config.RAG_FUSION_RRF_K)
        lines.append(f"  rrf_k      : {rrf}")
    if name == "hyde":
        lines.append(f"  doc_length : {per_strategy.get('doc_length', config.HYDE_DOC_LENGTH)}")
        lines.append(f"  include_orig: {per_strategy.get('include_original_query', config.HYDE_INCLUDE_ORIGINAL_QUERY)}")
    lines += [f"  available  : {', '.join(_REGISTRY.keys())}", "=" * 60, ""]
    print("\n".join(lines), flush=True)


__all__ = [
    "BaseRetriever",
    "PlainRetriever",
    "MultiQueryRetriever",
    "HyDERetriever",
    "RAGFusionRetriever",
    "RewriteRetriever",
    "get_retriever",
    "available_strategies",
    "log_active_strategy",
]
