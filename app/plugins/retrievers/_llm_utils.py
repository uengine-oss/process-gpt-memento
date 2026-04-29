"""retriever 내부에서 재사용하는 LLM 호출 유틸."""
import asyncio
from typing import List


async def ainvoke_text(llm, prompt: str) -> str:
    """
    create_llm()이 반환하는 ChatOpenAI를 비동기로 호출해 순수 텍스트만 돌려준다.

    LangChain의 ChatOpenAI는 .ainvoke가 있으면 그걸, 없으면 .invoke를 스레드에서
    실행해 폴백한다. 응답 형태가 AIMessage이면 .content를, 그 외에는 str()을 사용한다.
    """
    try:
        if hasattr(llm, "ainvoke"):
            msg = await llm.ainvoke(prompt)
        else:
            msg = await asyncio.to_thread(llm.invoke, prompt)
    except Exception as e:
        print(f"[retrievers] LLM 호출 실패: {e}")
        return ""

    content = getattr(msg, "content", None)
    return (content if isinstance(content, str) else str(msg)).strip()


def parse_numbered_list(text: str, max_items: int = 10) -> List[str]:
    """
    '1. foo\n2. bar\n- baz' 같은 줄 단위 출력에서 항목만 뽑는다.
    빈 줄/번호/불릿/공백을 제거한 실 문장들을 max_items 개까지 반환한다.
    """
    import re

    items: List[str] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        # 앞쪽의 "1.", "1)", "-", "*", "•" 등을 제거
        line = re.sub(r"^\s*(?:\d+[\.\)]|[-*•])\s*", "", line).strip()
        if line:
            items.append(line)
        if len(items) >= max_items:
            break
    return items
