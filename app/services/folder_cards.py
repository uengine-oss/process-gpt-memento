"""folder_cards — 계층 폴더 카드 생성/증분/백필.

폴더 트리가 지식베이스의 지도다. 각 폴더에 대해 "이 폴더에 무엇이 있고, 어떤 질문이면
무엇부터 열어야 하나"를 카드로 남긴다. 대규모(동일 골격의 여러 사업) 코퍼스에서 flat
임베딩이 cross-contamination 되므로 폴더 경로/요약이 구별 신호다.

설계 = **경량 하이브리드** (폐쇄망 약한 LLM 부담·환각 최소화):
- 결정론(코드): 문서 수, 날짜 범위, 문서 종류, 후보 엔티티 — DB/파일명에서 계산.
- LLM 1회/폴더: 요약 + 토픽 + 읽는 순서(reading_guide) + 시작 문서(start_with).

카드 생성은 *bottom-up*: 자식 문서 카드(``knowledge_doc_cards``) + 자식 폴더 카드를 모아
부모를 만든다. 비용은 *문서당이 아니라 폴더당 LLM 1회* 라 50GB 여도 저렴.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.core.supabase_client import supabase
from app.services.llm_output import parse_json_object

logger = logging.getLogger(__name__)

# LLM 프롬프트 토큰 방어 상한.
_MAX_DIRECT_FILES_IN_PROMPT = 40
_MAX_ABSTRACT_CHARS = 200
_MAX_CHILD_SUMMARIES = 20
_MAX_TOPICS = 8
_MAX_ENTITIES = 8
_MAX_GUIDE_LINES = 6
_MAX_START_WITH = 3
_IN_CHUNK = 150
# 백필/오픈 시 한 폴더 prefix 로 끌어올 row 상한.
_FETCH_LIMIT = 20_000


# ─────────────────────────────────────────────────────────────────────────────
# 경로 헬퍼 (api/folders.py 와 동일 규칙 — service 가 api 를 import 하지 않게 복제)
# ─────────────────────────────────────────────────────────────────────────────

def _norm(path: Optional[str]) -> str:
    return (path or "").strip().strip("/")


def _parent(path: str) -> Optional[str]:
    if not path or "/" not in path:
        return None
    return path.rsplit("/", 1)[0]


def _leaf(path: str) -> str:
    return path.rsplit("/", 1)[-1] if path else ""


def _ancestors(path: str) -> List[str]:
    if not path:
        return []
    parts = [p for p in path.split("/") if p]
    out, acc = [], []
    for p in parts:
        acc.append(p)
        out.append("/".join(acc))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 결정론 추출 — 날짜/종류/엔티티
# ─────────────────────────────────────────────────────────────────────────────

_EXT_LABEL = {
    "pdf": "PDF", "docx": "Word", "doc": "Word", "hwp": "한글", "hwpx": "한글",
    "pptx": "PPT", "ppt": "PPT", "txt": "텍스트", "md": "텍스트",
    "xlsx": "엑셀", "xls": "엑셀", "csv": "CSV",
    "jpg": "이미지", "jpeg": "이미지", "png": "이미지",
}

_YEAR_RE = re.compile(r"(?:19|20)\d{2}")
_YYMMDD_RE = re.compile(r"(?<!\d)(\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
_STOPWORDS = {
    "보고", "검토", "관련", "최종", "수정", "회의", "자료", "문서", "현황", "계획",
    "the", "and", "for", "with", "draft", "final", "report", "rev", "ver",
}


def _ext_of(file_name: str) -> str:
    if not file_name or "." not in file_name:
        return ""
    return file_name.rsplit(".", 1)[-1].lower()


def _extract_years(file_names: List[str], modified_times: List[str]) -> List[int]:
    years: set[int] = set()
    for nm in file_names:
        for m in _YEAR_RE.findall(nm or ""):
            try:
                years.add(int(m))
            except ValueError:
                pass
        for mt in _YYMMDD_RE.finditer(nm or ""):
            yy = int(mt.group(1))
            years.add(2000 + yy if yy < 70 else 1900 + yy)
    for mt in modified_times:
        if isinstance(mt, str) and len(mt) >= 4:
            head = mt[:4]
            if head.isdigit():
                years.add(int(head))
    # 1990~현재+1 범위로 필터 (노이즈 제거)
    now_y = datetime.utcnow().year + 1
    return sorted(y for y in years if 1990 <= y <= now_y)


def _date_range(years: List[int]) -> str:
    if not years:
        return ""
    return f"{years[0]}" if years[0] == years[-1] else f"{years[0]}~{years[-1]}"


def _doc_types(file_names: List[str]) -> List[str]:
    labels: List[str] = []
    for nm in file_names:
        lbl = _EXT_LABEL.get(_ext_of(nm))
        if lbl and lbl not in labels:
            labels.append(lbl)
    return labels


def _candidate_entities(file_names: List[str], abstracts: List[str]) -> List[str]:
    """파일명/abstract 에서 *반복 등장* 하는 토큰을 후보 엔티티로(휴리스틱).

    완벽한 NER 가 아니라 네비 힌트용. 영문 고유명사(대문자 시작)와 한글 명사 토큰을
    빈도순으로 추린다. 숫자·확장자·불용어 제외.
    """
    counter: Counter[str] = Counter()
    blob = " ".join(file_names) + "  " + " ".join(abstracts)
    # 영문 고유명사 후보 (Capitalized, 2자+)
    for tok in re.findall(r"[A-Z][A-Za-z]{1,}", blob):
        if tok.lower() in _STOPWORDS:
            continue
        counter[tok] += 1
    # 한글 토큰 (2자+)
    for tok in re.findall(r"[가-힣]{2,}", blob):
        if tok in _STOPWORDS:
            continue
        counter[tok] += 1
    # 2회 이상 등장한 것 우선, 빈도순 상위 N
    common = [t for t, c in counter.most_common(40) if c >= 2]
    return common[:_MAX_ENTITIES]


# ─────────────────────────────────────────────────────────────────────────────
# DB 조회
# ─────────────────────────────────────────────────────────────────────────────

async def _direct_files(tenant_id: str, folder_path: str) -> List[Dict[str, Any]]:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref, file_name, doc_card, mime_type, modified_time")
            .eq("tenant_id", tenant_id)
            .eq("folder_path", folder_path)
            .execute
        )
        return resp.data or []
    except Exception as e:
        logger.warning("[folder_cards] direct_files failed (%s): %s", folder_path, e)
        return []


async def _doc_card_rows(tenant_id: str, files: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """직속 문서들의 문서 카드 ``{file_id: {card, status, built_at}}``. 테이블이 없으면 빈 dict."""
    file_ids = [str(f.get("source_ref") or "") for f in files if f.get("source_ref")]
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(file_ids), _IN_CHUNK):
        chunk = file_ids[i:i + _IN_CHUNK]
        try:
            resp = await asyncio.to_thread(
                supabase.table("knowledge_doc_cards")
                .select("file_id, card, status, built_at")
                .eq("tenant_id", tenant_id)
                .in_("file_id", chunk)
                .execute
            )
        except Exception as e:
            logger.info("[folder_cards] doc card 조회 생략: %s", e)
            return out
        for row in resp.data or []:
            fid = str(row.get("file_id") or "")
            if fid:
                out[fid] = row
    return out


def _doc_brief(f: Dict[str, Any], row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """폴더 카드 프롬프트 한 줄에 들어갈 문서 요약. 새 카드가 없으면 구식 abstract 로."""
    card = (row or {}).get("card") if isinstance((row or {}).get("card"), dict) else {}
    legacy = f.get("doc_card") if isinstance(f.get("doc_card"), dict) else {}
    return {
        "file_name": f.get("file_name") or "?",
        "title": str(card.get("title") or "").strip(),
        "doc_type": str(card.get("doc_type") or "").strip(),
        "summary": str(card.get("summary") or legacy.get("abstract") or "").strip().replace("\n", " "),
        "distinguishers": [str(x) for x in (card.get("distinguishers") or []) if x],
        "status": (row or {}).get("status") or ("legacy" if legacy.get("abstract") else "pending"),
    }


def _card_signals(rows: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
    """자식 문서 카드에서 폴더 라우팅에 쓸 신호를 모은다.

    폴더 요약은 "무엇에 대한 폴더인가"만 답한다. 에이전트가 폴더를 고르려면 그 안의
    문서들이 *어떤 질문에 답하는지* 를 알아야 해서, 자식 카드의 topics/answers_questions
    를 빈도순으로 올려 준다.
    """
    topics: Counter[str] = Counter()
    questions: List[str] = []
    for row in rows.values():
        card = row.get("card") if isinstance(row.get("card"), dict) else {}
        for topic in (card.get("topics") or [])[:8]:
            text = str(topic or "").strip()
            if text:
                topics[text] += 1
        for question in (card.get("answers_questions") or [])[:4]:
            text = str(question or "").strip()
            if text and text not in questions:
                questions.append(text)
    return {
        "topics": [topic for topic, _ in topics.most_common(_MAX_TOPICS)],
        "answers_questions": questions[:12],
    }


async def _descendant_folder_paths(tenant_id: str, folder_path: str) -> List[str]:
    """folder_path 하위(직접 아님 포함)의 distinct folder_path 들."""
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("folder_path")
            .eq("tenant_id", tenant_id)
            .like("folder_path", f"{folder_path}/%")
            .limit(_FETCH_LIMIT)
            .execute
        )
        return [_norm(r.get("folder_path")) for r in (resp.data or []) if _norm(r.get("folder_path"))]
    except Exception as e:
        logger.warning("[folder_cards] descendant_folders failed (%s): %s", folder_path, e)
        return []


async def _direct_child_folders(tenant_id: str, folder_path: str) -> List[str]:
    prefix = folder_path + "/"
    seen: set[str] = set()
    for dp in await _descendant_folder_paths(tenant_id, folder_path):
        if dp.startswith(prefix):
            seg = dp[len(prefix):].split("/", 1)[0]
            seen.add(prefix + seg)
    return sorted(seen)


async def _get_card_row(tenant_id: str, folder_path: str) -> Optional[Dict[str, Any]]:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_folder_cards")
            .select("folder_path, card, signature")
            .eq("tenant_id", tenant_id)
            .eq("folder_path", folder_path)
            .limit(1)
            .execute
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception as e:
        logger.debug("[folder_cards] get_card_row failed (table missing?): %s", e)
        return None


async def _all_folders_for_tenant(tenant_id: str) -> List[str]:
    """knowledge_files folder_path 들에서 파생한 *모든 폴더(조상 포함)* 집합."""
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("folder_path")
            .eq("tenant_id", tenant_id)
            .limit(_FETCH_LIMIT)
            .execute
        )
        folders: set[str] = set()
        for r in (resp.data or []):
            fp = _norm(r.get("folder_path"))
            for anc in _ancestors(fp):
                folders.add(anc)
        return sorted(folders)
    except Exception as e:
        logger.warning("[folder_cards] all_folders failed: %s", e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# signature — 증분 재생성 판정
# ─────────────────────────────────────────────────────────────────────────────

def _doc_card_version(f: Dict[str, Any], row: Optional[Dict[str, Any]]) -> str:
    if row:
        return f"{row.get('status') or ''}@{row.get('built_at') or ''}"
    card = f.get("doc_card")
    if isinstance(card, dict):
        return str(card.get("generated_at") or card.get("abstract", "")[:16] or "")
    return ""


def _compute_signature(
    direct_files: List[Dict[str, Any]],
    card_rows: Dict[str, Dict[str, Any]],
    child_sigs: List[Tuple[str, str]],
) -> str:
    parts: List[str] = ["v2"]
    for f in sorted(direct_files, key=lambda r: str(r.get("source_ref") or "")):
        ref = str(f.get("source_ref") or "")
        parts.append(f"F|{ref}|{_doc_card_version(f, card_rows.get(ref))}")
    for cfp, csig in sorted(child_sigs):
        parts.append(f"D|{cfp}|{csig}")
    blob = "\n".join(parts)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# LLM 요약 (1회/폴더)
# ─────────────────────────────────────────────────────────────────────────────

_FOLDER_JSON_SHAPE = """\
{"summary": "이 폴더가 무엇을 담는지 2~4문장. 첫 문장은 이 폴더를 형제 폴더와 구별하는 사실(사업명·발주처·연도·단계)로 시작",
 "topics": ["핵심 토픽 5~8개"],
 "reading_guide": ["<어떤 질문이나 목적이면> → <먼저 열 문서 파일명 또는 하위 폴더명>. 3~6줄"],
 "start_with": ["전체 맥락을 가장 빨리 잡게 해 주는 시작 지점 1~3개. 위 목록의 문서 파일명 또는 하위 폴더명을 그대로 쓸 것(없는 이름은 버려진다)"]}
"""


def _build_summary_prompt(
    folder_name: str,
    briefs: List[Dict[str, Any]],
    child_cards: List[Tuple[str, Dict[str, Any]]],
) -> str:
    lines: List[str] = []
    if briefs:
        lines.append("[이 폴더의 문서들]")
        for b in briefs[:_MAX_DIRECT_FILES_IN_PROMPT]:
            head = f"- {b['file_name']}"
            if b["doc_type"]:
                head += f" ({b['doc_type']})"
            if b["title"] and b["title"] != b["file_name"]:
                head += f" · {b['title']}"
            if b["summary"]:
                head += f" — {b['summary'][:_MAX_ABSTRACT_CHARS]}"
            if b["distinguishers"]:
                head += f" | 구별: {', '.join(b['distinguishers'][:4])}"
            lines.append(head)
    if child_cards:
        lines.append("")
        lines.append("[하위 폴더]")
        for cname, ccard in child_cards[:_MAX_CHILD_SUMMARIES]:
            csum = str(ccard.get("summary") or "").strip().replace("\n", " ")[:_MAX_ABSTRACT_CHARS]
            topics = [str(t) for t in (ccard.get("topics") or [])[:5]]
            line = f"- {cname}/"
            if csum:
                line += f": {csum}"
            if topics:
                line += f" (토픽: {', '.join(topics)})"
            lines.append(line)
    body = "\n".join(lines)
    return (
        f"당신은 자료실 사서다. 다음은 '{folder_name}' 폴더의 문서들과 하위 폴더다.\n"
        "이 폴더를 처음 여는 사람이 무엇부터 읽어야 하는지 알 수 있게 아래 JSON 을 채워라.\n"
        f"{_FOLDER_JSON_SHAPE}\n"
        "규칙:\n"
        "- 여기 적힌 사실만 쓴다. 추측·일반 지식 보충 금지.\n"
        "- reading_guide 의 화살표 오른쪽은 위 목록에 실제로 있는 파일명 또는 하위 폴더명만 쓴다.\n"
        "- 문서가 하나뿐이면 reading_guide 는 그 문서로 가는 한 줄이면 된다.\n"
        "- 모든 내용은 문서들의 언어를 따른다. JSON 객체만 출력한다. 코드펜스·설명 금지.\n\n"
        f"{body}\n\n[JSON]"
    )


def _clean_list(values: Any, *, limit: int, max_chars: int = 200) -> List[str]:
    out: List[str] = []
    if not isinstance(values, list):
        return out
    for v in values:
        text = str(v or "").strip()
        if text and text not in out and len(text) <= max_chars:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def _parse_summary_output(text: str) -> Dict[str, Any]:
    payload = parse_json_object(text) if isinstance(text, str) else None
    if not isinstance(payload, dict):
        # 형식 안 지킨 경우: 전체를 요약으로 (코드펜스 제거)
        cleaned = (text or "").strip().strip("`").strip() if isinstance(text, str) else ""
        return {"summary": cleaned[:600], "topics": [], "reading_guide": [], "start_with": []}
    return {
        "summary": str(payload.get("summary") or "").strip()[:800],
        "topics": _clean_list(payload.get("topics"), limit=_MAX_TOPICS, max_chars=60),
        "reading_guide": _clean_list(payload.get("reading_guide"), limit=_MAX_GUIDE_LINES),
        "start_with": _clean_list(payload.get("start_with"), limit=_MAX_START_WITH, max_chars=255),
    }


async def _generate_summary(
    folder_name: str,
    briefs: List[Dict[str, Any]],
    child_cards: List[Tuple[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    empty = {"summary": "", "topics": [], "reading_guide": [], "start_with": []}
    if not briefs and not child_cards:
        return empty
    prompt = _build_summary_prompt(folder_name, briefs, child_cards)
    try:
        from app.services.llm import create_llm
        llm = create_llm(temperature=0.0, timeout=(10.0, 90.0), max_retries=2)
        resp = await llm.ainvoke(prompt)
        raw = getattr(resp, "content", resp)
        if not isinstance(raw, str):
            raw = str(raw)
        parsed = _parse_summary_output(raw)
        known = {b["file_name"] for b in briefs} | {f"{c}/" for c, _ in child_cards} | {c for c, _ in child_cards}
        # start_with 는 파일명 그대로여야 화면·에이전트가 그 문서로 갈 수 있다.
        parsed["start_with"] = [s for s in parsed["start_with"] if s in known]
        return parsed
    except Exception as e:
        logger.warning("[folder_cards] summary LLM failed (%s): %s", folder_name, e)
        return empty


def _resolve_generation_model() -> str:
    try:
        from app.core.config import resolve_llm_config
        return str(resolve_llm_config().get("model") or "")
    except Exception:
        return ""


def _resolve_start_with(
    raw: List[str], file_names: List[str], child_folders: List[str]
) -> List[str]:
    """LLM 이 고른 시작 지점을 *실제로 존재하는* 이름으로만 남긴다.

    없는 이름이 남으면 화면은 죽은 칩을, 에이전트는 열 수 없는 경로를 받는다.
    직속 문서가 없는 상위 폴더에서는 하위 폴더가 올바른 시작 지점이므로 같이 허용하고,
    소비자가 문서와 구분할 수 있게 ``이름/`` 형태로 통일한다.
    """
    files = {n: n for n in file_names if n}
    folders = {_leaf(p): _leaf(p) + "/" for p in child_folders if _leaf(p)}
    out: List[str] = []
    for item in raw or []:
        name = str(item or "").strip().strip("/")
        if not name:
            continue
        resolved = files.get(name) or folders.get(name)
        if resolved and resolved not in out:
            out.append(resolved)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 카드 빌드 + 증분 + 백필
# ─────────────────────────────────────────────────────────────────────────────

async def build_folder_card(
    tenant_id: str,
    folder_path: str,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    """폴더 1개 카드 빌드 후 upsert. 반환 ``{"folder_path","changed","skipped","signature"}``.

    signature 동일하고 force=False 면 LLM 호출·쓰기 모두 skip (멱등·저비용).
    """
    fp = _norm(folder_path)
    if not tenant_id or not fp:
        return {"folder_path": fp, "changed": False, "skipped": True, "signature": ""}

    direct = await _direct_files(tenant_id, fp)
    child_folders = await _direct_child_folders(tenant_id, fp)

    # 빈 폴더(직속 문서 0 + 하위폴더 0) — 삭제됐거나 비워진 폴더. 카드 행 제거 후 종료.
    if not direct and not child_folders:
        await _delete_card_row(tenant_id, fp)
        return {"folder_path": fp, "changed": True, "skipped": False, "deleted": True, "signature": ""}

    # 자식 폴더의 기존 카드(요약 + signature) 수집 — bottom-up 이므로 이미 빌드돼 있어야 정확.
    child_sigs: List[Tuple[str, str]] = []
    child_cards: List[Tuple[str, Dict[str, Any]]] = []
    for cfp in child_folders:
        row = await _get_card_row(tenant_id, cfp)
        csig = (row or {}).get("signature") or ""
        ccard = (row or {}).get("card") if isinstance((row or {}).get("card"), dict) else {}
        child_sigs.append((cfp, csig))
        child_cards.append((_leaf(cfp), ccard or {}))

    card_rows = await _doc_card_rows(tenant_id, direct)
    signature = _compute_signature(direct, card_rows, child_sigs)

    existing = await _get_card_row(tenant_id, fp)
    if existing and existing.get("signature") == signature and not force:
        return {"folder_path": fp, "changed": False, "skipped": True, "signature": signature}

    briefs = [_doc_brief(f, card_rows.get(str(f.get("source_ref") or ""))) for f in direct]

    # 결정론 필드
    file_names = [f.get("file_name") or "" for f in direct]
    summaries = [b["summary"] for b in briefs if b["summary"]]
    modified_times = [str(f.get("modified_time") or "") for f in direct]
    years = _extract_years(file_names, modified_times)

    # 하위 전체 문서 수 = 직접(folder_path==fp) + 하위(folder_path like fp/%).
    n_docs_total = len(direct) + await _count_descendant_files(tenant_id, fp)

    # LLM 요약 (1회)
    generated = await _generate_summary(_leaf(fp) or fp, briefs, child_cards)
    signals = _card_signals(card_rows)
    status_counts = Counter(b["status"] for b in briefs)

    card: Dict[str, Any] = {
        "summary": generated["summary"],
        "topics": signals["topics"] or generated["topics"],
        "reading_guide": generated["reading_guide"],
        "start_with": _resolve_start_with(generated["start_with"], file_names, child_folders),
        "answers_questions": signals["answers_questions"],
        "doc_types": _doc_types(file_names),
        "date_range": _date_range(years),
        "key_entities": _candidate_entities(file_names, summaries),
        "n_docs_direct": len(direct),
        "n_docs_total": n_docs_total,
        "n_subfolders": len(child_folders),
        # 직속 문서 카드 준비 상태 — 지도가 얼마나 채워졌는지 화면과 에이전트가 같은 숫자를 본다.
        "cards": {
            "done": status_counts.get("done", 0) + status_counts.get("legacy", 0),
            "pending": status_counts.get("pending", 0),
            "failed": status_counts.get("failed", 0),
            "empty": status_counts.get("empty", 0),
        },
        # child_cards[i] 는 child_folders[i] 에 대응 (위 루프에서 동순서로 append).
        "subfolders": [{"name": nm, "one_liner": (c or {}).get("summary") or ""} for (nm, c) in child_cards],
        "built_at": datetime.utcnow().isoformat() + "Z",
        "signature": signature,
        "model": _resolve_generation_model(),
    }

    try:
        await asyncio.to_thread(
            supabase.table("knowledge_folder_cards")
            .upsert(
                {
                    "tenant_id": tenant_id,
                    "folder_path": fp,
                    "card": card,
                    "signature": signature,
                    "built_at": card["built_at"],
                },
                on_conflict="tenant_id,folder_path",
            )
            .execute
        )
    except Exception as e:
        logger.warning("[folder_cards] upsert failed (%s): %s", fp, e)
        return {"folder_path": fp, "changed": False, "skipped": False, "signature": signature,
                "error": str(e)}

    logger.info(
        "[folder_cards] built %s direct=%d total=%d subfolders=%d topics=%d guide=%d",
        fp, len(direct), n_docs_total, len(child_folders), len(card["topics"]),
        len(card["reading_guide"]),
    )
    return {"folder_path": fp, "changed": True, "skipped": False, "signature": signature}


async def _count_descendant_files(tenant_id: str, folder_path: str) -> int:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref", count="exact")
            .eq("tenant_id", tenant_id)
            .like("folder_path", f"{folder_path}/%")
            .limit(1)
            .execute
        )
        return int(getattr(resp, "count", 0) or 0)
    except Exception:
        return 0


async def rebuild_card_with_propagation(tenant_id: str, folder_path: str) -> int:
    """폴더 카드 빌드 후, 카드가 *바뀌면* 부모로 올라가며 재빌드. 안 바뀌면 중단.

    경로 깊이만큼만(보통 ~7) 도므로 증분 비용 bounded. 반환: 빌드된 폴더 수.
    """
    fp = _norm(folder_path)
    built = 0
    cur: Optional[str] = fp
    while cur:
        res = await build_folder_card(tenant_id, cur)
        built += 1
        if res.get("skipped") and not res.get("changed"):
            # 이 폴더 카드가 안 바뀜 → 상위도 안 바뀜 → 중단.
            break
        cur = _parent(cur)
    return built


async def _delete_card_row(tenant_id: str, folder_path: str) -> None:
    """단일 폴더 카드 행 삭제 (정확히 그 folder_path)."""
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_folder_cards")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("folder_path", folder_path)
            .execute
        )
    except Exception as e:
        logger.debug("[folder_cards] delete card row failed (%s): %s", folder_path, e)


async def delete_folder_cards(tenant_id: str, folder_path: str) -> int:
    """폴더 + 그 하위(subtree)의 카드 행을 모두 삭제. 폴더 삭제 시 호출.

    반환: 삭제 시도한 그룹 수(대략).
    """
    fp = _norm(folder_path)
    if not tenant_id or not fp:
        return 0
    n = 0
    for q in (
        # 정확히 그 폴더
        lambda r: r.eq("folder_path", fp),
        # 하위 폴더들
        lambda r: r.like("folder_path", f"{fp}/%"),
    ):
        try:
            base = (
                supabase.table("knowledge_folder_cards")
                .delete()
                .eq("tenant_id", tenant_id)
            )
            await asyncio.to_thread(q(base).execute)
            n += 1
        except Exception as e:
            logger.warning("[folder_cards] delete_folder_cards failed (%s): %s", fp, e)
    logger.info("[folder_cards] deleted cards under %s", fp)
    return n


async def rebuild_folders(tenant_id: str, folder_paths: List[str]) -> Dict[str, Any]:
    """주어진 폴더들 + 그 *조상 전부* 를 bottom-up(잎부터)으로 1회씩 재생성.

    업로드/파일삭제 배치 후 *영향받은 폴더만* 갱신하는 경로 (full backfill 대비 저렴, storm 없음).
    각 폴더는 signature-skip 으로 실제 바뀐 것만 LLM 호출. 같은 폴더는 한 번만 빌드(dedupe).
    """
    affected: set[str] = set()
    for raw in folder_paths or []:
        fp = _norm(raw)
        if not fp:
            continue
        for anc in _ancestors(fp):
            affected.add(anc)
    if not affected:
        return {"built": 0, "skipped": 0, "deleted": 0}

    # 잎(깊은) 폴더부터 → 부모가 자식 카드를 집계할 때 이미 최신.
    ordered = sorted(affected, key=lambda p: p.count("/"), reverse=True)
    built = skipped = deleted = 0
    for fp in ordered:
        res = await build_folder_card(tenant_id, fp)
        if res.get("deleted"):
            deleted += 1
        elif res.get("changed"):
            built += 1
        else:
            skipped += 1
    logger.info(
        "[folder_cards] rebuild_folders affected=%d built=%d skipped=%d deleted=%d",
        len(affected), built, skipped, deleted,
    )
    return {"built": built, "skipped": skipped, "deleted": deleted}


async def backfill_tenant(tenant_id: str) -> Dict[str, Any]:
    """tenant 의 모든 폴더 카드를 bottom-up(잎부터)으로 1회 빌드. signature 동일이면 skip."""
    folders = await _all_folders_for_tenant(tenant_id)
    # 깊은(잎) 폴더부터 → 부모가 자식 카드를 집계할 때 이미 존재.
    folders.sort(key=lambda p: p.count("/"), reverse=True)
    total_built = 0
    total_skipped = 0
    for fp in folders:
        res = await build_folder_card(tenant_id, fp)
        if res.get("changed"):
            total_built += 1
        else:
            total_skipped += 1
    logger.info("[folder_cards] backfill folders=%d", len(folders))
    return {"tenant_id": tenant_id, "built": total_built, "skipped": total_skipped}


