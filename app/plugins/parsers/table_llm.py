"""PDF 표 영역을 LLM 으로 다시 읽는다(MEMENTO_TABLE_LLM). 근거·측정: docs/DESIGN_NOTES.md "PDF 표: 규칙 대 LLM".

find_tables 가 찾은 표마다 영역 그림과 그 영역의 PDF 글자를 함께 주고 마크다운 표를 받는다. 받은 표의 숫자가 모두
PDF 글자에 있을 때만 쓴다 — 아니면 규칙으로 만든 표를 그대로 둔다.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import config, vision

PROMPT = """이 그림은 PDF 의 표 하나다(위아래에 제목·단위·본문 줄이 걸쳐 있을 수 있다). 아래 [글자]는 같은 영역의 PDF 텍스트 레이어다.
표를 마크다운 표로 옮겨라.
- 숫자와 낱말은 [글자]에 있는 그대로 쓴다. 고치거나 계산하지 않는다.
- 병합 칸은 덮인 칸마다 같은 값을 쓴다. 묶음 이름(연도, 상위 항목)은 하위 행마다 반복한다.
- 머리행이 여러 줄이면 "상위 > 하위"로 합쳐 한 줄로 쓴다.
- 표 밖의 제목·단위·주석은 쓰지 않는다. 표가 아니면 아무것도 쓰지 않는다.
- 표만 출력한다.

[글자]
"""
_DPI = 150
_ABOVE, _BELOW = 25, 5  # 제목·단위 줄까지 보이게 위로 조금 넓힌다
_MAX_TOKENS = 8000
_NUM = re.compile(r"[△▲▽▼+\-−]?\d[\d,]*(?:\.\d+)?")
_YEAR_MONTH = re.compile(r"\d{4}\.\d{1,2}")  # 따로 적힌 연·월을 "2024.3" 으로 합쳐 쓰는 것은 지어낸 값이 아니다


def enabled() -> bool:
    return config.TABLE_LLM


def page_tasks(page, page_num: int, text_items) -> Tuple[List[Tuple[str, Callable[[], str]]], List[Dict[str, Any]]]:
    """이 쪽의 표마다 (작업 키, 호출) 와 교체에 쓸 정보. text_items 에 실제로 들어간 표만."""
    from .pymupdf_parser import PyMuPDFParser

    placed = {tuple(bbox) for _, text, bbox in text_items if text.lstrip().startswith("|")}
    tasks, infos = [], []
    for idx, (bbox, _) in enumerate(PyMuPDFParser._tables(page)):
        if tuple(bbox) not in placed:
            continue
        x0, y0, x1, y1 = bbox
        r = page.rect
        clip = (r.x0 + 15, max(r.y0, y0 - _ABOVE), r.x1 - 15, min(r.y1, y1 + _BELOW))  # 가로는 쪽 폭: 표 영역이 이름 열을 빼먹는 PDF 가 있다
        try:
            png = page.get_pixmap(dpi=_DPI, clip=clip).tobytes("png")
            text = page.get_text("text", clip=clip)
        except Exception:
            continue
        key = f"tbl:{page_num}:{idx}"
        tasks.append((key, (lambda b=png, t=text: vision._vlm_call(b, PROMPT + t, "image/png", _MAX_TOKENS, role="table"))))
        infos.append({"key": key, "bbox": list(bbox), "text": text})
    return tasks, infos


def accept(md: str, text: str) -> Optional[str]:
    """LLM 표를 쓸지. 표 행이 둘 이상이고 숫자가 모두 영역 글자에 있으면 표 행만, 아니면 None."""
    rows = [ln.strip() for ln in (md or "").splitlines() if ln.strip().startswith("|")]
    if len(rows) < 2:
        return None
    hay = re.sub(r"\s", "", text).replace("−", "-")
    for tok in _NUM.findall("\n".join(rows)):
        tok = tok.replace("−", "-")
        if tok not in hay and not _YEAR_MONTH.fullmatch(tok.lstrip("+-")):
            return None
    return "\n".join(rows)


def apply(entries, infos: List[Dict[str, Any]], results: Dict[str, str]):
    """entries [(순서 키, text, bbox)] 의 표를 받아들인 LLM 표로 바꾼다. (entries, 바꾼 수, 버린 수)."""
    by_bbox = {}
    used = dropped = 0
    for info in infos:
        md = accept(results.get(info["key"], ""), info["text"])
        if md:
            by_bbox[tuple(info["bbox"])] = md
            used += 1
        else:
            dropped += 1
    if not by_bbox:
        return entries, used, dropped
    out = [(k, by_bbox.get(tuple(b), t) if t.lstrip().startswith("|") else t, b) for k, t, b in entries]
    return out, used, dropped
