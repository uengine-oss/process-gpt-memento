"""Three-case test for memento_retrieve through a real LangGraph ReAct loop.

We construct a minimal ReAct agent (not the full WorkAssistantAgent — that
needs Supabase/MCP) wired with ONLY the memento_retrieve tool from base-agent's
local_tools.py. Then we exercise the agent with three user prompts and verify
the tool-call sequence + resulting answer for each case.

Cases:
  1) No file mention -> exactly 1 retrieve call with NO file_name (broad query)
  2) Single file mention (by alias / ordinal) -> 1 retrieve with that file's name
  3) Two files with distinct roles -> 2 retrieves, one per file_name
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

# Make memento and base-agent importable
ROOT = Path(__file__).resolve().parent
BASE_AGENT_SRC = ROOT.parent / "process-gpt-base-agent-langchain-react" / "src"
sys.path.insert(0, str(BASE_AGENT_SRC))

os.environ.setdefault("MEMENTO_SERVICE_URL", "http://127.0.0.1:8005")

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from work_assistant_agent.local_tools import build_local_tools  # type: ignore


FILE_A = "Contract_A_NovaCloud_SaaS_Agreement.md"
FILE_B = "Contract_B_Helios_Cloud_Services_Agreement.md"

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://ai-server.dream-flow.com:30000/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "frentis")
LLM_MODEL = os.getenv("LLM_MODEL", "frentis-ai-model")


@dataclass
class Ctx:
    tenant_id: str = "test-tenant"


def build_system_prompt(attached: List[Dict[str, str]]) -> str:
    """Mirror the relevant subset of base-agent's system prompt for this test."""
    files_block = "\n".join(
        f"  {i+1}. fileName={f['fileName']}  alias={f.get('alias','')}"
        for i, f in enumerate(attached)
    ) or "  (no files attached)"
    return f"""당신은 업무 문서 분석 어시스턴트입니다.

[AttachedFiles] (현재 대화에 첨부된 문서, ordinal=목록 순서)
{files_block}

도구:
- memento_retrieve(query, file_name=None, top_k=5, all_docs=True):
  사내 문서 RAG 검색. 호출 규칙은 docstring 의 1)/2)/3) 을 그대로 따른다.

[중요 결정 규칙]
* 사용자가 어떤 파일도 언급하지 않으면 file_name 인자를 절대 채우지 말 것 (전체 검색).
* 사용자가 한 파일을 언급하면 [AttachedFiles] 에서 정확한 fileName 으로 매핑해 호출.
* 사용자가 두 파일을 각각의 역할로 언급하면 두 번 별도 호출 후 비교.
* "첫 번째/두 번째" 같은 ordinal 도 위 [AttachedFiles] 순서대로 매핑할 것.
* 동일한 의도로 같은 파일을 두 번 호출하지 말 것.

답변은 한국어로 작성하되, 어떤 파일에서 어떤 단서를 찾았는지 명시하세요.
"""


def make_agent(attached: List[Dict[str, str]]):
    llm = ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0.0,
        timeout=180,
    )
    tools = [t for t in build_local_tools(lambda: Ctx()) if t.name == "memento_retrieve"]
    return create_react_agent(llm, tools=tools)


def collect_tool_calls(messages: List[Any]) -> List[Dict[str, Any]]:
    """Pull every tool_call out of the AI messages in run order."""
    calls: List[Dict[str, Any]] = []
    for m in messages:
        if isinstance(m, AIMessage) and getattr(m, "tool_calls", None):
            for tc in m.tool_calls:
                calls.append({"name": tc["name"], "args": tc.get("args", {})})
    return calls


def final_text(messages: List[Any]) -> str:
    for m in reversed(messages):
        if isinstance(m, AIMessage) and not getattr(m, "tool_calls", None):
            return m.content if isinstance(m.content, str) else str(m.content)
    return "<no final answer>"


async def run_case(
    label: str,
    user_msg: str,
    attached: List[Dict[str, str]],
    expect: Dict[str, Any],
) -> Dict[str, Any]:
    print(f"\n{'='*72}\n{label}\n{'='*72}")
    print(f"USER: {user_msg}\n")
    agent = make_agent(attached)
    sys_prompt = build_system_prompt(attached)
    state = await agent.ainvoke(
        {"messages": [SystemMessage(sys_prompt), HumanMessage(user_msg)]},
        config={"recursion_limit": 12},
    )
    msgs = state["messages"]
    calls = collect_tool_calls(msgs)
    answer = final_text(msgs)

    print("TOOL CALLS:")
    for i, c in enumerate(calls, 1):
        fn = c["args"].get("file_name") or "<none>"
        q = c["args"].get("query", "")
        print(f"  {i}. memento_retrieve(file_name={fn!r}, query={q!r})")

    # Assertions
    n = expect.get("num_calls")
    file_names = expect.get("file_names")  # list of expected file_names per call (None == no filter)
    errors: List[str] = []

    if n is not None and len(calls) != n:
        errors.append(f"expected {n} retrieve call(s), got {len(calls)}")

    if file_names is not None:
        actual = [c["args"].get("file_name") for c in calls]
        # order-agnostic compare (set semantics)
        if sorted([str(x) for x in actual]) != sorted([str(x) for x in file_names]):
            errors.append(f"expected file_names={file_names}, got {actual}")

    print("\nANSWER (truncated):")
    print(answer[:600])
    if errors:
        print("\n❌ FAIL:")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\n✅ PASS")
    return {"label": label, "errors": errors, "calls": calls}


async def main() -> int:
    attached = [
        {"fileName": FILE_A, "alias": "Contract A / NovaCloud / 첫 번째 파일"},
        {"fileName": FILE_B, "alias": "Contract B / Helios / 두 번째 파일"},
    ]

    results = []

    results.append(
        await run_case(
            "CASE 1 — 파일 미언급 (전체 문서 검색)",
            "첨부된 문서들에서 데이터 보호 및 보안 사고 통보와 관련한 핵심 조항을 정리해줘.",
            attached,
            expect={"num_calls": 1, "file_names": [None]},
        )
    )

    results.append(
        await run_case(
            "CASE 2 — 한 파일 언급 (ordinal/alias 매핑)",
            "첫 번째 파일(NovaCloud 계약서)의 보안 사고 통보 조항만 발췌해줘.",
            attached,
            expect={"num_calls": 1, "file_names": [FILE_A]},
        )
    )

    results.append(
        await run_case(
            "CASE 3 — 두 파일 각자 역할로 언급 (비교)",
            "Contract A 와 Contract B 의 정보보안 프로그램 / 사고 통보 시한을 조항별로 비교해줘. "
            "각 계약서에서 해당 조항을 따로 찾아 비교 표를 만들어 줘.",
            attached,
            expect={"num_calls": 2, "file_names": [FILE_A, FILE_B]},
        )
    )

    print(f"\n\n{'='*72}\nSUMMARY\n{'='*72}")
    failed = [r for r in results if r["errors"]]
    for r in results:
        status = "PASS" if not r["errors"] else "FAIL"
        print(f"  [{status}] {r['label']}")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
