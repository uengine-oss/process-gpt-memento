"""용어 사전 추출 프롬프트.

doc_role='glossary' 자료 ingest 시 page batch 단위로 LLM 에 호출.
각 batch 결과를 받아 단순 concat 으로 정제본을 만든다(merge/dedup 없음).

★ 형식 강제 X ★ — 사용자가 어떤 사전(한↔영, 영문 약어집, 한국어 정의집,
사내 용어집 등)을 올릴지 모르므로 출력 형식은 LLM 이 자료 보고 결정.
우리는 추출 가드(추측 금지·산문 무시·한 줄 = 한 용어)만 부여.
"""
from __future__ import annotations


_CORE_RULES = """\
당신은 용어 사전 압축기입니다.
아래는 한 *용어 사전 자료* 의 일부 페이지 본문입니다.
이 페이지에 등장하는 *용어와 그 정의·번역·약어·동의어*만 압축해 정리합니다.

## 가드 (반드시 지킬 것)

1. 자료 본문에 *명시적으로 적힌* 내용만 추출. 추측·일반 지식 보충 금지.
2. 본문 설명·서문·머리말·목차·페이지번호·각주·예시 문단은 무시.
3. **한 줄 = 한 용어 단위** (다운스트림이 행 단위로 읽음).
4. 정확 표현 보존: 한글·영문·약어·정의 모두 자료에 적힌 *글자 그대로*.
   대소문자·괄호·하이픈·번역어 임의 변경 금지.
5. 같은 용어가 본문에 여러 번 등장해도 *한 줄만* 출력.

## 형식

출력 형식은 자료 성격에 맞춰 *너가 선택*하라.
한 batch 안에서는 *일관*되게 하나의 형식만 사용.

자료 성격에 따라 자연스러운 형식 예:
- 약어 위주 사전:  ``ABBR — Full Name (한글명) — 짧은 정의``
- 한↔영 매핑 사전: ``한글 → English (ABBR) — 정의``
- 한국어 정의집:   ``용어: 정의``
- 회사 내부 용어:  ``용어 — 짧은 의미``

위는 예시일 뿐 — 자료에 더 자연스러운 형식 있으면 그걸 써라.

## 출력

정리된 용어 줄들*만* 출력. 표 헤더·코드펜스·설명문·머리말 금지.
빈 응답 금지. 추출할 용어가 정말 0개면 ``[추출 가능 용어 없음]`` 한 줄.
"""


def build_glossary_extract_prompt(
    *,
    doc_abstract: str,
    n_pages: int,
    batch_start: int,
    batch_end: int,
    page_count: int,
    batch_text: str,
) -> str:
    """한 batch 추출 프롬프트 빌드.

    Args:
        doc_abstract: 사전 전체 문서의 abstract (있으면, glossary 는 보통 없음).
        n_pages: 사전 전체 페이지 수 (참고).
        batch_start / batch_end: 이 batch 의 페이지 범위.
        page_count: 이 batch 의 페이지 개수.
        batch_text: 이 batch 의 페이지 본문 (## p.N 헤더 포함).
    """
    abstract_block = ""
    if doc_abstract and doc_abstract.strip():
        abstract_block = f"\n## 사전 전체 개요\n\n{doc_abstract.strip()}\n"

    return (
        f"{_CORE_RULES}\n"
        f"## 이 batch 정보\n"
        f"- 사전 총 페이지: {n_pages}\n"
        f"- 이 batch 페이지 범위: p.{batch_start}–p.{batch_end} ({page_count}페이지)\n"
        f"{abstract_block}\n"
        f"## 본문 (이 batch 페이지)\n\n"
        f"{batch_text}\n\n"
        f"## 출력\n"
        f"위 본문에서 추출한 용어 줄들을 *그대로* 출력하세요."
    )
