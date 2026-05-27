"""요약 프롬프트 템플릿 모듈.

용도:
  1. ``summary_service.summarize_document`` 의 mini-summary 생성
  2. (향후) ingest 시 ``document_pages.post_load_hook`` 의 doc_card abstract 생성
     — 같은 코어 지시문을 공유해서 *어떻게 요약해야 하는가* 의 기준을 한 곳에서 관리.

각 템플릿은 plain Python ``str.format`` 로 채움. 시스템 프롬프트와 user 콘텐츠를
한 문자열로 합쳐 단일 LLM 호출에 넘기는 것을 가정.
"""
from __future__ import annotations


# 모든 요약 작업이 공유하는 코어 원칙. 새 프롬프트 짤 때 이걸 참고/포함.
CORE_PRINCIPLES = """\
- 자료에 *명시적으로 등장한 사실* 만 추출. 추측·일반 지식 보충 절대 금지.
- 정확 표현(수치·단위·법령조항·고유명사·따옴표 안 표현)은 *글자 그대로* 인용.
  예: "60 m³", "POSRV 4기", "±0.5 bar", "10 CFR Part 52 Appendix N".
- 자료에 없는 *비교·차이·분류·종합 진술* 을 만들지 마라.
- 환각 없이 *비어 있다*고 적는 것이 *지어내는 것* 보다 안전하다.
"""


# ─────────────────────────────────────────────────────────────────────────────
# Mini-summary — 페이지 batch 단위 요약
#
# 입력: 한 batch (예: p.21-40) 의 본문 + 문서 전체 abstract 힌트
# 출력: 4~8 개 한국어 불릿
# ─────────────────────────────────────────────────────────────────────────────

MINI_SUMMARY_PROMPT_TEMPLATE = """\
당신은 긴 문서의 *일부 페이지 범위* 를 받아 그 부분의 핵심을 정리하는 사서입니다.

[문서 개요]
{doc_abstract}

[지금 너의 담당 범위]
이 문서 총 {n_pages}페이지 중 *p.{batch_start}-{batch_end}* (본 batch {page_count}페이지).

[지시]
이 페이지 범위의 핵심 사실을 *한국어 불릿 4~8개* 로 정리해라.

규칙:
{core_principles}
- 다른 페이지 범위는 보지 못한다 — *이 범위 내에서만* 정리. 전체 결론·종합 진술 작성 금지.
- 표·리스트·수치는 가능한 그대로 보존.
- 불릿 머리는 "- " 로 시작.
- 코드펜스(```) 사용 금지. 답변 본문만 출력.

[자료 — p.{batch_start}-{batch_end}]
{batch_text}

[정리]
"""


def build_mini_summary_prompt(
    doc_abstract: str,
    n_pages: int,
    batch_start: int,
    batch_end: int,
    page_count: int,
    batch_text: str,
) -> str:
    """mini-summary 프롬프트 1개 생성."""
    return MINI_SUMMARY_PROMPT_TEMPLATE.format(
        doc_abstract=(doc_abstract or "(이 문서 전체 개요는 별도로 제공되지 않음)"),
        n_pages=n_pages,
        batch_start=batch_start,
        batch_end=batch_end,
        page_count=page_count,
        batch_text=batch_text,
        core_principles=CORE_PRINCIPLES,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Doc-card abstract — ingest 시 단일 호출
#
# (현재 document_pages.py 에 인라인으로 있음 — 향후 이 모듈에서 import 형태로 통합 가능)
# ─────────────────────────────────────────────────────────────────────────────

DOC_CARD_ABSTRACT_PROMPT_TEMPLATE = """\
다음은 어떤 문서의 앞부분과 마지막 페이지 일부다.
이 문서가 무엇인지 *1~2 문장* 의 한국어 평문으로 적어라.

규칙:
{core_principles}
- 코드펜스·JSON·따옴표·"요약:" 같은 머리말 없이 *답변 문장만* 출력.

[자료]
{sample_text}

[이 문서의 요약]
"""


def build_doc_card_abstract_prompt(sample_text: str) -> str:
    """doc_card abstract 프롬프트 생성."""
    return DOC_CARD_ABSTRACT_PROMPT_TEMPLATE.format(
        sample_text=sample_text,
        core_principles=CORE_PRINCIPLES,
    )
