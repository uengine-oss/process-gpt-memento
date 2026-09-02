"""계약서 구조화기 — LLM 전문 구조탐지 (poc_nda/contract_structurer.py production 포팅).

대원칙: 추출(결정적: docx XML→블록+메모앵커) ≠ 해석(LLM: 전문 읽고 조항경계/배경/
contract_type 를 *블록 인덱스로* 반환) ≠ 재구성(인덱스 슬라이싱→원문 그대로, 환각·메모유실 0).
번호스타일(제N조/Article/1.1/무번호)·언어·배경위치를 하드코딩하지 않는다.

LLM: memento 설정(resolve_llm_config) 의 모델/엔드포인트. raw httpx + enable_thinking=False
(frentis/Qwen 계열 reasoning 차단 — memento create_llm 은 thinking 을 끄지 않으므로 직접 호출).
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from app.core.config import resolve_llm_config
from app.plugins.parsers.docx_structured import parse as _docx_parse

logger = logging.getLogger(__name__)


# ── LLM ────────────────────────────────────────────────────────────────────
def _llm(prompt: str, max_tokens: int = 4096, temperature: float = 0.0, retries: int = 3) -> str:
    cfg = resolve_llm_config()
    url = cfg["base_url"].rstrip("/") + "/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {cfg['api_key']}"}
    if cfg.get("extra_headers"):
        headers.update(cfg["extra_headers"])
    payload = {
        "model": cfg["model"],
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    last_err = None
    for _ in range(retries):
        try:
            with httpx.Client(timeout=300.0) as client:
                resp = client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                body = resp.json()
            msg = (body.get("choices") or [{}])[0].get("message") or {}
            content = msg.get("content") or msg.get("reasoning_content") or ""
            if isinstance(content, list):
                content = "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in content)
            return str(content).strip()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
    logger.warning("[legal_review] LLM 호출 실패: %s", last_err)
    return ""


def _extract_json(text: str) -> Optional[Any]:
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[-1]
        if t.endswith("```"):
            t = t[: t.rfind("```")]
    for open_c, close_c in (("{", "}"), ("[", "]")):
        s, e = t.find(open_c), t.rfind(close_c)
        if s != -1 and e > s:
            try:
                return json.loads(t[s : e + 1])
            except Exception:  # noqa: BLE001
                continue
    return None


# ── 결정적 추출: 순서있는 통합 블록 ────────────────────────────────────────
def _resolve_block_comments(cs, meta) -> List[Dict[str, Any]]:
    out = []
    for c in cs or []:
        m = meta.get(c["id"]) or {}
        out.append({
            "id": c["id"], "author": m.get("author", ""), "date": m.get("date", ""),
            "anchor_text": c.get("anchor_text", ""), "body": (m.get("text") or "").strip(),
        })
    return out


def _build_unified_blocks(parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
    meta = parsed.get("comments") or {}
    blocks: List[Dict[str, Any]] = []
    for i, b in enumerate(parsed.get("blocks") or []):
        if b.get("type") == "paragraph":
            blocks.append({
                "idx": i, "kind": "paragraph", "text": (b.get("text") or "").strip(),
                "comments": _resolve_block_comments(b.get("comments"), meta),
            })
        elif b.get("type") == "table":
            comments = []
            for row in b.get("rows") or []:
                for cell in row:
                    comments.extend(_resolve_block_comments(cell.get("comments"), meta))
            blocks.append({
                "idx": i, "kind": "table", "text": (b.get("markdown") or "").strip(),
                "comments": comments,
            })
    return blocks


# ── LLM-A: 전문 구조 탐지 ──────────────────────────────────────────────────
_TOPIC_HINTS = (
    "정의, 비밀유지의무, 비밀의범위, 사용제한, 예외, 반환, 지식재산권, 손해배상, "
    "진술보장, 계약기간, 해지, 준거법, 분쟁해결, 양도, 통지, 수정, 권리포기, "
    "가분성, 비용, 수출통제, 관계부인, 완전합의"
)

_STRUCTURE_PROMPT = (
    "너는 계약서 구조 분석기다. 아래는 하나의 계약서(docx)를 문단/표 단위로 "
    "순서대로 나눈 *블록 목록*이다. 각 줄 앞 [n] 은 블록 인덱스다.\n\n"
    "다음을 판단해 **JSON으로만** 답하라:\n"
    '- "contract_type": 계약 종류를 영문 소문자 한 단어로 (nda, mou, service, sale, '
    'license, jv, loan, employment ... 중 가장 가까운 것. 모르면 "other")\n'
    '- "language": "ko" | "en" | "mixed"\n'
    '- "background_block_indices": 사업/거래 *배경·목적·당사자 소개*(recitals, WHEREAS, '
    "'목적' 조 등)에 해당하는 블록 인덱스 배열\n"
    '- "clauses": 조항 배열. 각 원소 = '
    '{"clause_no","title","topic","block_indices"}\n'
    '    - "clause_no": 문서에 표기된 조 번호 (없으면 순번 문자열)\n'
    '    - "title": 조항 제목 (없으면 본문 요지로 짧게)\n'
    '    - "topic": 조항 핵심 주제 한국어 라벨. 가능하면 다음 표준 어휘 사용 → '
    f"[{_TOPIC_HINTS}]. 해당 없으면 자유롭게 한국어로.\n"
    '    - "block_indices": 그 조항(제목+본문+딸린 표)에 해당하는 블록 인덱스 배열\n\n'
    "규칙:\n"
    "- 제목/서명란/날짜/빈 블록 등 본문이 아닌 건 어디에도 안 넣어도 된다.\n"
    "- 번호 스타일(제3조 / Article 3 / 3.1 / 무번호 굵은제목)에 구애받지 말고 *의미*로 판단.\n"
    "- 한 블록은 최대 한 조항에만. 배경과 조항은 겹치지 않게.\n"
    "- **원문을 절대 바꾸지 마라. 너는 인덱스만 배정한다.**\n"
    "- 모든 조항을 빠짐없이. 길어도 전부 처리.\n\n"
    "출력 예시:\n"
    '{"contract_type":"nda","language":"en","background_block_indices":[1,2,3],'
    '"clauses":[{"clause_no":"1","title":"Confidential Information","topic":"정의",'
    '"block_indices":[5,6]}]}\n\n'
)


def _blocks_for_prompt(blocks: List[Dict[str, Any]]) -> str:
    lines = []
    for b in blocks:
        t = b["text"].replace("\n", " ⏎ ")
        tag = "표" if b["kind"] == "table" else "문단"
        lines.append(f"[{b['idx']}] ({tag}) {t}")
    return "\n".join(lines)


def _detect_structure(blocks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    listing = _blocks_for_prompt(blocks)
    prompt = _STRUCTURE_PROMPT + "[블록 목록]\n" + listing + "\n\nJSON만 출력:"
    return _extract_json(_llm(prompt, max_tokens=8192))


def _validate_structure(data: Any, n_blocks: int) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None

    def _ints(v):
        return sorted({i for i in (v or []) if isinstance(i, int) and 0 <= i < n_blocks})

    clauses = []
    for c in data.get("clauses") or []:
        if not isinstance(c, dict):
            continue
        bis = _ints(c.get("block_indices"))
        title = str(c.get("title", "")).strip()
        if not bis and not title:
            continue
        clauses.append({
            "clause_no": str(c.get("clause_no", "")).strip(),
            "title": title,
            "topic": str(c.get("topic", "")).strip() or "기타",
            "block_indices": bis,
        })
    return {
        "contract_type": str(data.get("contract_type", "other")).strip().lower() or "other",
        "language": str(data.get("language", "")).strip().lower(),
        "background_block_indices": _ints(data.get("background_block_indices")),
        "clauses": clauses,
    }


def _assemble(blocks: List[Dict[str, Any]], struct: Dict[str, Any]):
    by_idx = {b["idx"]: b for b in blocks}
    assigned: set = set()

    bg_idx = [i for i in struct["background_block_indices"] if i in by_idx]
    bg_raw = "\n".join(by_idx[i]["text"] for i in bg_idx if by_idx[i]["text"])
    assigned.update(bg_idx)

    clauses = []
    for c in struct["clauses"]:
        idxs = [i for i in c["block_indices"] if i in by_idx and i not in assigned]
        assigned.update(idxs)
        text = "\n".join(by_idx[i]["text"] for i in idxs if by_idx[i]["text"]).strip()
        memos = [m for i in idxs for m in by_idx[i]["comments"]]
        clauses.append({
            "clause_no": c["clause_no"], "title": c["title"],
            "topic": c["topic"], "text": text, "memos": memos,
        })

    other = [m for b in blocks if b["idx"] not in assigned for m in b["comments"]]
    return bg_raw, clauses, other


# ── LLM-B: 사업배경 정규화 (도메인 무관) ───────────────────────────────────
_BG_PROMPT = (
    "다음은 계약서 서두(당사자·배경·목적/recitals)다. 여기서 *거래/사업 배경*을 추출해 "
    "아래 JSON 스키마로만 답하라. 원문에 없으면 빈 문자열/빈 배열. 추측 금지. "
    "특정 산업에 국한된 필드를 만들지 말고 아래 일반 필드만 채워라.\n\n"
    "{\n"
    '  "industry": "산업/분야 (예: 전력발전, 소프트웨어, 건설, 방산, 금융)",\n'
    '  "parties": ["계약 당사자 법인명 + 약칭"],\n'
    '  "subject": "거래/계약 대상 (무엇에 관한 거래인가)",\n'
    '  "purpose": "이 계약(특히 비밀유지/협력)의 목적",\n'
    '  "region": "국가/지역 (있으면)",\n'
    '  "key_terms": ["사업배경의 핵심 키워드 (고유명사·규모·기술 등)"],\n'
    '  "summary": "사업배경 1~2문장 한국어 요약 — 유사 계약 검색 매칭에 쓰임"\n'
    "}\n\n"
    "[계약서 서두]\n"
)


def _normalize_background(bg_raw: str) -> Dict[str, Any]:
    if not bg_raw.strip():
        return {"industry": "", "parties": [], "subject": "", "purpose": "",
                "region": "", "key_terms": [], "summary": ""}
    p = _extract_json(_llm(_BG_PROMPT + bg_raw + "\n\nJSON만 출력:", max_tokens=1024))
    if not isinstance(p, dict):
        p = {}
    for k in ("industry", "subject", "purpose", "region", "summary"):
        p.setdefault(k, "")
    p.setdefault("parties", [])
    p.setdefault("key_terms", [])
    return p


# ── 진입점 ─────────────────────────────────────────────────────────────────
def structure_contract(path: str) -> Dict[str, Any]:
    """docx 경로 → 구조화 결과 dict (doc_id/contract_type/business_background/clauses/...)."""
    parsed = _docx_parse(path, describe=False)
    n_blocks = len(parsed.get("blocks") or [])
    blocks = _build_unified_blocks(parsed)

    struct = _validate_structure(_detect_structure(blocks), n_blocks)

    if not struct or not struct["clauses"]:
        logger.warning("[legal_review] 구조탐지 실패/빈 결과 → 폴백(단일 조항): %s", path)
        all_text = "\n".join(b["text"] for b in blocks if b["text"])
        all_memos = [m for b in blocks for m in b["comments"]]
        return {
            "doc_id": Path(path).stem, "file_name": Path(path).name,
            "contract_type": "other", "language": "",
            "business_background": {"raw": "", "profile": _normalize_background("")},
            "clauses": [{"clause_no": "1", "title": "(전체)", "topic": "기타",
                         "text": all_text, "memos": all_memos}],
            "other_memos": [],
            "stats": {"clause_count": 1, "memo_count": len(all_memos),
                      "fallback": True, "block_count": n_blocks},
        }

    bg_raw, clauses, other_memos = _assemble(blocks, struct)
    profile = _normalize_background(bg_raw)
    total_memos = sum(len(c["memos"]) for c in clauses) + len(other_memos)
    return {
        "doc_id": Path(path).stem,
        "file_name": Path(path).name,
        "contract_type": struct["contract_type"],
        "language": struct["language"],
        "business_background": {"raw": bg_raw, "profile": profile},
        "clauses": clauses,
        "other_memos": other_memos,
        "stats": {
            "clause_count": len(clauses), "memo_count": total_memos,
            "other_memo_count": len(other_memos), "block_count": n_blocks, "fallback": False,
        },
    }
