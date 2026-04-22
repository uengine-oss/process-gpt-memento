"""
청킹 전략 레지스트리.

활성 전략과 파라미터는 `chunkers/config.py`에서 설정합니다.
해당 파일을 수정한 뒤 서비스를 재시작하면 전략이 교체됩니다.

사용 가능한 전략:
  - recursive        (기본값, 현재 동작)
  - fixed_token      (토큰 수 기반 기계적 baseline)
  - markdown_header  (헤더 구조 보존)
  - semantic         (임베딩 유사도 기반 경계 감지)
  - hybrid           (markdown_header → recursive 2차 분할, 권장)

알 수 없는 전략명이 들어오면 `recursive`로 자동 폴백하므로
폐쇄망에서 오타가 나도 ingest가 죽지 않습니다.
"""
from typing import Dict, Type

from . import config
from .base import BaseChunker
from .recursive import RecursiveChunker
from .fixed_token import FixedTokenChunker
from .markdown_header import MarkdownHeaderChunker
from .semantic import SemanticChunker
from .hybrid import HybridChunker


_REGISTRY: Dict[str, Type[BaseChunker]] = {
    RecursiveChunker.name: RecursiveChunker,
    FixedTokenChunker.name: FixedTokenChunker,
    MarkdownHeaderChunker.name: MarkdownHeaderChunker,
    SemanticChunker.name: SemanticChunker,
    HybridChunker.name: HybridChunker,
}


def available_strategies() -> list[str]:
    return list(_REGISTRY.keys())


def get_chunker(
    strategy: str | None = None,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
) -> BaseChunker:
    """
    청커 인스턴스 팩토리.

    인자는 명시적 오버라이드용(선택). 생략하면 chunkers/config.py 값을 사용하며,
    PER_STRATEGY_OVERRIDES에 해당 전략이 등록돼 있으면 그 값이 우선 적용된다.
    """
    name = (strategy or config.STRATEGY or "recursive").strip().lower()
    cls = _REGISTRY.get(name)
    if cls is None:
        print(f"[chunkers] 알 수 없는 전략 '{name}' → 'recursive'로 폴백")
        cls = RecursiveChunker
        name = "recursive"

    per_strategy = config.PER_STRATEGY_OVERRIDES.get(name, {}) or {}

    size = (
        chunk_size
        if chunk_size is not None
        else per_strategy.get("chunk_size", config.CHUNK_SIZE)
    )
    overlap = (
        chunk_overlap
        if chunk_overlap is not None
        else per_strategy.get("chunk_overlap", config.CHUNK_OVERLAP)
    )

    kwargs: dict = {}
    if cls is SemanticChunker:
        kwargs["similarity_threshold"] = config.SEMANTIC_SIM_THRESHOLD
        kwargs["min_sentences_per_chunk"] = config.SEMANTIC_MIN_SENTENCES

    print(f"[chunkers] '{name}' 전략 사용 (chunk_size={size}, chunk_overlap={overlap})")
    return cls(chunk_size=size, chunk_overlap=overlap, **kwargs)


def log_active_strategy() -> None:
    """서버 시작 시 현재 선택된 청킹 전략을 로깅한다."""
    name = (config.STRATEGY or "recursive").strip().lower()
    if name not in _REGISTRY:
        name = "recursive"

    per_strategy = config.PER_STRATEGY_OVERRIDES.get(name, {}) or {}
    size = per_strategy.get("chunk_size", config.CHUNK_SIZE)
    overlap = per_strategy.get("chunk_overlap", config.CHUNK_OVERLAP)

    lines = [
        "",
        "=" * 60,
        " Chunking configuration",
        "=" * 60,
        f"  strategy      : {name}",
        f"  chunk_size    : {size}",
        f"  chunk_overlap : {overlap}",
    ]
    if name == "semantic":
        lines.append(f"  sim_threshold : {config.SEMANTIC_SIM_THRESHOLD}")
        lines.append(f"  min_sentences : {config.SEMANTIC_MIN_SENTENCES}")
    lines += [f"  available     : {', '.join(_REGISTRY.keys())}", "=" * 60, ""]
    print("\n".join(lines), flush=True)


__all__ = [
    "BaseChunker",
    "RecursiveChunker",
    "FixedTokenChunker",
    "MarkdownHeaderChunker",
    "SemanticChunker",
    "HybridChunker",
    "get_chunker",
    "available_strategies",
    "log_active_strategy",
]
