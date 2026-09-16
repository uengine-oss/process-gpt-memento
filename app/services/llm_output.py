"""모델 출력에서 원하는 것만 꺼내는 자리.

요청은 ``app.services.llm`` 이, 응답 해석은 여기가 맡는다. 같은 일을 하는 파서가
호출부마다 조금씩 다르게 있으면 한쪽만 고쳐지므로 한 벌만 둔다.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional


def message_text(data: Dict[str, Any]) -> str:
    """``/chat/completions`` 응답의 본문. reasoning 모델은 필드가 다르다."""
    choices = data.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content") or message.get("reasoning_content") or ""
    if isinstance(content, list):
        content = "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in content)
    return str(content)


def strip_wrapping_fence(text: str) -> str:
    """출력 전체를 감싼 ```...``` 만 벗긴다. 본문 중간의 코드블록은 건드리지 않는다.

    "표는 마크다운으로" 같은 지시를 받으면 모델이 페이지 전체를 코드펜스로 감싸는 일이
    잦다. 그대로 저장하면 본문 전체가 코드블록이 되어 표/제목이 렌더되지 않는다.
    """
    if not text:
        return text
    body = text.strip()
    if not body.startswith("```"):
        return body
    newline = body.find("\n")
    if newline == -1:
        return body  # 한 줄뿐이면 펜스로 보지 않는다
    opener = body[:newline].strip()  # "```" 또는 "```markdown"
    if not (opener == "```" or opener.strip("`").strip().isalnum()):
        return body
    rest = body[newline + 1:]
    if not rest.rstrip().endswith("```"):
        return body
    return rest.rstrip()[:-3].strip()


def parse_json(raw: Any) -> Optional[Any]:
    """모델 출력에서 JSON 만 건져낸다. 실패는 실패로 남긴다(추측 금지)."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = strip_wrapping_fence(raw)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # 설명文에 섞여 나온 경우 — 가장 바깥 괄호쌍만 다시 시도한다.
    for opener, closer in (("{", "}"), ("[", "]")):
        start, end = text.find(opener), text.rfind(closer)
        if start != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                continue
    return None


def parse_json_object(raw: Any) -> Optional[Dict[str, Any]]:
    """``parse_json`` 중 객체만 받는다. 배열·스칼라는 실패로 본다."""
    value = parse_json(raw)
    return value if isinstance(value, dict) else None
