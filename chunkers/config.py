"""
청킹 설정 — 이 파일만 수정하면 전략이 바뀝니다.

  1. `kubectl exec -it <pod> -- bash`  (또는 `docker exec -it ...`)
  2. 이 파일에서 STRATEGY 및 관련 파라미터 수정
  3. 서비스 재시작 (DocumentProcessor 생성 시점에 값을 읽기 때문)

사용 가능한 전략:
  "recursive"        : 기존 동작 (안전한 기본값)
  "fixed_token"      : tiktoken 기반 기계적 분할 (baseline 비교용)
  "markdown_header"  : # / ## / ### 헤더 기준 분할, section_path 메타 보존
  "semantic"         : 임베딩 유사도 기반 경계 감지 (임베딩 호출 많음)
  "hybrid"           : markdown_header 우선 → 초과 청크만 recursive 재분할 (권장)
"""

# --- 활성 전략 -------------------------------------------------------------
STRATEGY: str = "recursive"


# --- 공통 파라미터 (대부분 문자 단위, fixed_token만 토큰 단위) --------------
CHUNK_SIZE: int = 2000
CHUNK_OVERLAP: int = 400


# --- 전략별 파라미터 -------------------------------------------------------
# STRATEGY 값에 해당하는 항목만 사용되고, 나머지는 무시됨.

SEMANTIC_SIM_THRESHOLD: float = 0.75   # 낮을수록 청크가 커짐, 높을수록 작아짐
SEMANTIC_MIN_SENTENCES: int = 2        # 청크당 최소 문장 수


# 전략별로 chunk_size/overlap을 다르게 실험하고 싶을 때 사용.
# 비어 있으면 위의 공통 값(CHUNK_SIZE, CHUNK_OVERLAP)을 그대로 씀.
PER_STRATEGY_OVERRIDES: dict = {
    # "fixed_token":     {"chunk_size": 500,  "chunk_overlap": 100},
    # "markdown_header": {"chunk_size": 2500, "chunk_overlap": 300},
    # "semantic":        {"chunk_size": 1800, "chunk_overlap": 0},
    # "hybrid":          {"chunk_size": 2000, "chunk_overlap": 400},
}
