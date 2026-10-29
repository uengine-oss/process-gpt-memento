"""글의 언어 — 카드·요약을 문서와 같은 말로 쓰게 하는 데 쓴다.

프롬프트와 JSON 예시가 한국어라 「문서의 언어를 따른다」 규칙만으로는 영어 문서에도
한국어 요약·태그·문서 종류(예: 명세서)가 붙었다. 영어 자료면 그 사실을 프롬프트에 직접 적는다.
"""
from __future__ import annotations

import re

_HANGUL = re.compile(r"[가-힣]")
_LATIN = re.compile(r"[A-Za-z]")


def text_language(text: str) -> str:
    """영문자가 20자 이상이고 한글의 10배를 넘으면 'en', 아니면 'ko'(기본)."""
    t = str(text or "")
    hangul, latin = len(_HANGUL.findall(t)), len(_LATIN.findall(t))
    return "en" if latin >= 20 and hangul * 10 < latin else "ko"


def english_output_rule(text: str, fields: str) -> str:
    """영어 자료일 때만 붙이는 규칙 한 줄(한국어 자료면 빈 문자열)."""
    if text_language(text) != "en":
        return ""
    return f"- 이 자료는 영어다. {fields} 를 모두 영어로 쓴다 — 위 예시가 한국어여도 영어로.\n"


def bullet_language_word(text: str) -> str:
    """요약 지시문의 「한국어 불릿」 자리에 들어갈 말."""
    return "영어" if text_language(text) == "en" else "한국어"
