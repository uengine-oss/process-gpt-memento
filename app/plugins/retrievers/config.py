"""
Retrieval 설정.

사용 가능한 전략:
  "plain" : 쿼리를 그대로 임베딩해 top-k 검색 (LLM 호출 없음)

top_k 는 /search·/retrieve 의 쿼리 파라미터로 호출 측이 정한다.
"""

STRATEGY: str = "plain"
