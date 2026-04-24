"""
Retrieval 설정 — 이 파일만 수정하면 검색 전략이 바뀝니다.

폐쇄망 운영 절차:
  1. `kubectl exec -it <pod> -- bash`
  2. 이 파일에서 STRATEGY 및 관련 파라미터 수정
  3. 서비스 재시작 (RAGChain.retrieve 호출 시점에 값을 읽기 때문)

사용 가능한 전략:
  "plain"        : 쿼리를 그대로 임베딩해 top-k 검색 (기본값, LLM 호출 없음)
  "multi_query"  : LLM이 쿼리를 N개 패러프레이즈 → 병합·dedupe
  "hyde"         : LLM이 가상 답변을 생성 → 그 답변으로 검색
  "rag_fusion"   : multi_query + Reciprocal Rank Fusion 재순위
  "rewrite"      : LLM이 검색 친화적으로 쿼리 재작성 → 한 번 검색
"""

# --- 활성 전략 -------------------------------------------------------------
STRATEGY: str = "plain"


# 참고: top_k는 /retrieve 엔드포인트의 쿼리 파라미터로 호출 측에서 지정한다
# (main.py의 retrieve 엔드포인트 → rag_chain.retrieve → get_retriever).
# 따라서 config에는 top_k를 두지 않는다.


# --- 전략별 파라미터 -------------------------------------------------------
# multi_query / rag_fusion: 확장 쿼리 개수 (원본 쿼리는 별도로 항상 포함됨)
MULTI_QUERY_COUNT: int = 3

# rag_fusion: RRF 상수 (보통 60 권장. 낮을수록 상위 랭크 가중치 ↑)
RAG_FUSION_RRF_K: int = 60

# hyde: LLM이 생성할 가상 답변 길이 힌트
HYDE_DOC_LENGTH: str = "1-2 sentences"
# hyde: True이면 원본 쿼리와 가상 답변 둘 다로 검색해 병합 (recall ↑, 비용 ↑)
HYDE_INCLUDE_ORIGINAL_QUERY: bool = False


# 전략별 파라미터 오버라이드 (top_k는 여기서 다루지 않음 — 엔드포인트 파라미터)
PER_STRATEGY_OVERRIDES: dict = {
    # "rag_fusion":  {"query_count": 4, "rrf_k": 60},
    # "hyde":        {"doc_length": "2-3 sentences", "include_original_query": True},
}
