"""folder_cards — 계층 폴더 카드 생성/증분/백필.

폴더 트리를 1급 인덱스로 삼는 knowledge-navigator 의 *수집 단계* 로직. 각 폴더에 대해
"이 폴더에 무엇이 있나"를 카드로 남긴다. 대규모(동일 골격의 여러 사업) 코퍼스에서 flat
임베딩이 cross-contamination 되므로 폴더 경로/요약이 구별 신호다.

설계 = **경량 하이브리드** (폐쇄망 약한 LLM 부담·환각 최소화):
- 결정론(코드): 문서 수, 날짜 범위, 문서 종류, 후보 엔티티 — DB/파일명에서 계산.
- LLM 1회/폴더: 요약 2~4문장 + 토픽 목록만.

카드 생성은 *bottom-up*: 자식 doc_card(abstract, 이미 존재) + 자식 폴더 카드(summary)를 모아
부모를 만든다. 비용은 *문서당이 아니라 폴더당 LLM 1회* 라 50GB 여도 저렴.
``document_pages._generate_abstract`` 패턴을 미러.
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
from app.services.knowledge_files import normalize_doc_role

logger = logging.getLogger(__name__)

# LLM 프롬프트 토큰 방어 상한.
_MAX_DIRECT_FILES_IN_PROMPT = 40
_MAX_ABSTRACT_CHARS = 200
_MAX_CHILD_SUMMARIES = 20
_MAX_TOPICS = 8
_MAX_ENTITIES = 8
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

async def _direct_files(tenant_id: str, folder_path: str, doc_role: str) -> List[Dict[str, Any]]:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref, file_name, doc_card, mime_type, modified_time")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", doc_role)
            .eq("folder_path", folder_path)
            .execute
        )
        return resp.data or []
    except Exception as e:
        logger.warning("[folder_cards] direct_files failed (%s/%s): %s", folder_path, doc_role, e)
        return []


async def _child_card_signals(tenant_id: str, files: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """자식 문서 카드에서 폴더 라우팅에 쓸 신호를 모은다.

    폴더 요약은 "무엇에 대한 폴더인가"만 답한다. 에이전트가 폴더를 고르려면 그 안의
    문서들이 *어떤 질문에 답하는지* 를 알아야 해서, 자식 카드의 topics/answers_questions
    를 빈도순으로 올려 준다. ``knowledge_doc_cards`` 가 없으면 빈 값(폴백).
    """
    file_ids = [str(f.get("source_ref") or "") for f in files if f.get("source_ref")]
    if not file_ids:
        return {"topics": [], "answers_questions": []}
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_doc_cards")
            .select("card")
            .eq("tenant_id", tenant_id)
            .in_("file_id", file_ids)
            .execute
        )
        rows = resp.data or []
    except Exception as e:
        logger.info("[folder_cards] doc card 신호 조회 생략: %s", e)
        return {"topics": [], "answers_questions": []}

    topics: Counter[str] = Counter()
    questions: List[str] = []
    for row in rows:
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


async def _descendant_folder_paths(tenant_id: str, folder_path: str, doc_role: str) -> List[str]:
    """folder_path 하위(직접 아님 포함)의 distinct folder_path 들."""
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("folder_path")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", doc_role)
            .like("folder_path", f"{folder_path}/%")
            .limit(_FETCH_LIMIT)
            .execute
        )
        return [_norm(r.get("folder_path")) for r in (resp.data or []) if _norm(r.get("folder_path"))]
    except Exception as e:
        logger.warning("[folder_cards] descendant_folders failed (%s): %s", folder_path, e)
        return []


async def _direct_child_folders(tenant_id: str, folder_path: str, doc_role: str) -> List[str]:
    prefix = folder_path + "/"
    seen: set[str] = set()
    for dp in await _descendant_folder_paths(tenant_id, folder_path, doc_role):
        if dp.startswith(prefix):
            seg = dp[len(prefix):].split("/", 1)[0]
            seen.add(prefix + seg)
    return sorted(seen)


async def _get_card_row(tenant_id: str, folder_path: str, doc_role: str) -> Optional[Dict[str, Any]]:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_folder_cards")
            .select("folder_path, card, signature")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", doc_role)
            .eq("folder_path", folder_path)
            .limit(1)
            .execute
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception as e:
        logger.debug("[folder_cards] get_card_row failed (table missing?): %s", e)
        return None


async def _all_folders_for_tenant(tenant_id: str, doc_role: str) -> List[str]:
    """knowledge_files folder_path 들에서 파생한 *모든 폴더(조상 포함)* 집합."""
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("folder_path")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", doc_role)
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

def _doc_card_version(row: Dict[str, Any]) -> str:
    card = row.get("doc_card")
    if isinstance(card, dict):
        return str(card.get("generated_at") or card.get("abstract", "")[:16] or "")
    return ""


def _compute_signature(
    direct_files: List[Dict[str, Any]],
    child_sigs: List[Tuple[str, str]],
) -> str:
    parts: List[str] = []
    for f in sorted(direct_files, key=lambda r: str(r.get("source_ref") or "")):
        parts.append(f"F|{f.get('source_ref')}|{_doc_card_version(f)}")
    for cfp, csig in sorted(child_sigs):
        parts.append(f"D|{cfp}|{csig}")
    blob = "\n".join(parts)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# LLM 요약 (1회/폴더)
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary_prompt(
    folder_name: str,
    direct_files: List[Dict[str, Any]],
    child_summaries: List[Tuple[str, str]],
) -> str:
    lines: List[str] = []
    if direct_files:
        lines.append("[이 폴더의 문서들]")
        for f in direct_files[:_MAX_DIRECT_FILES_IN_PROMPT]:
            nm = f.get("file_name") or "?"
            card = f.get("doc_card") if isinstance(f.get("doc_card"), dict) else {}
            ab = (card.get("abstract") or "").strip().replace("\n", " ")[:_MAX_ABSTRACT_CHARS]
            lines.append(f"- {nm}" + (f" — {ab}" if ab else ""))
    if child_summaries:
        lines.append("")
        lines.append("[하위 폴더 요약]")
        for cname, csum in child_summaries[:_MAX_CHILD_SUMMARIES]:
            csum = (csum or "").strip().replace("\n", " ")[:_MAX_ABSTRACT_CHARS]
            lines.append(f"- {cname}: {csum}" if csum else f"- {cname}")
    body = "\n".join(lines)
    return (
        f"다음은 '{folder_name}' 폴더에 들어 있는 문서들과 하위 폴더 요약이다.\n"
        "이 폴더가 *무엇을 담고 있는 폴더인지* 2~4문장의 한국어 평문으로 설명하고,\n"
        "핵심 토픽을 쉼표로 구분해 5~8개 나열하라.\n"
        "여기 없는 사실을 추가하지 마라. 추측 금지. 아래 형식만 출력하라:\n"
        "요약: <2~4문장>\n"
        "토픽: <토픽1, 토픽2, ...>\n\n"
        f"{body}\n\n"
        "출력:"
    )


def _parse_summary_output(text: str) -> Tuple[str, List[str]]:
    summary = ""
    topics: List[str] = []
    if not isinstance(text, str):
        return summary, topics
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("요약:"):
            summary = line[len("요약:"):].strip()
        elif line.startswith("토픽:"):
            tpart = line[len("토픽:"):].strip()
            topics = [t.strip() for t in re.split(r"[,/·]", tpart) if t.strip()][:_MAX_TOPICS]
    # 형식 안 지킨 경우: 전체를 요약으로 (코드펜스 제거)
    if not summary:
        cleaned = text.strip().strip("`").strip()
        summary = cleaned[:600]
    return summary, topics


async def _generate_summary(
    folder_name: str,
    direct_files: List[Dict[str, Any]],
    child_summaries: List[Tuple[str, str]],
) -> Tuple[str, List[str]]:
    if not direct_files and not child_summaries:
        return "", []
    prompt = _build_summary_prompt(folder_name, direct_files, child_summaries)
    try:
        from app.services.llm import create_llm
        llm = create_llm(temperature=0.0, timeout=(10.0, 60.0), max_retries=2)
        resp = await llm.ainvoke(prompt)
        raw = getattr(resp, "content", resp)
        if not isinstance(raw, str):
            raw = str(raw)
        return _parse_summary_output(raw)
    except Exception as e:
        logger.warning("[folder_cards] summary LLM failed (%s): %s", folder_name, e)
        return "", []


def _resolve_generation_model() -> str:
    try:
        from app.core.config import resolve_llm_config
        return str(resolve_llm_config().get("model") or "")
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# 카드 빌드 + 증분 + 백필
# ─────────────────────────────────────────────────────────────────────────────

async def build_folder_card(
    tenant_id: str,
    folder_path: str,
    doc_role: str = "content",
    *,
    force: bool = False,
) -> Dict[str, Any]:
    """폴더 1개 카드 빌드 후 upsert. 반환 ``{"folder_path","changed","skipped","signature"}``.

    signature 동일하고 force=False 면 LLM 호출·쓰기 모두 skip (멱등·저비용).
    """
    fp = _norm(folder_path)
    role = normalize_doc_role(doc_role)
    if not tenant_id or not fp:
        return {"folder_path": fp, "changed": False, "skipped": True, "signature": ""}

    direct = await _direct_files(tenant_id, fp, role)
    child_folders = await _direct_child_folders(tenant_id, fp, role)

    # 빈 폴더(직속 문서 0 + 하위폴더 0) — 삭제됐거나 비워진 폴더. 카드 행 제거 후 종료.
    if not direct and not child_folders:
        await _delete_card_row(tenant_id, fp, role)
        return {"folder_path": fp, "changed": True, "skipped": False, "deleted": True, "signature": ""}

    # 자식 폴더의 기존 카드(요약 + signature) 수집 — bottom-up 이므로 이미 빌드돼 있어야 정확.
    child_sigs: List[Tuple[str, str]] = []
    child_summaries: List[Tuple[str, str]] = []
    for cfp in child_folders:
        row = await _get_card_row(tenant_id, cfp, role)
        csig = (row or {}).get("signature") or ""
        ccard = (row or {}).get("card") if isinstance((row or {}).get("card"), dict) else {}
        child_sigs.append((cfp, csig))
        child_summaries.append((_leaf(cfp), (ccard or {}).get("summary") or ""))

    signature = _compute_signature(direct, child_sigs)

    existing = await _get_card_row(tenant_id, fp, role)
    if existing and existing.get("signature") == signature and not force:
        return {"folder_path": fp, "changed": False, "skipped": True, "signature": signature}

    # 결정론 필드
    file_names = [f.get("file_name") or "" for f in direct]
    abstracts = []
    for f in direct:
        c = f.get("doc_card") if isinstance(f.get("doc_card"), dict) else {}
        if c.get("abstract"):
            abstracts.append(str(c["abstract"]))
    modified_times = [str(f.get("modified_time") or "") for f in direct]
    years = _extract_years(file_names, modified_times)

    # 하위 전체 문서 수 = 직접(folder_path==fp) + 하위(folder_path like fp/%).
    n_docs_total = len(direct) + await _count_descendant_files(tenant_id, fp, role)

    # LLM 요약 (1회)
    summary, topics = await _generate_summary(_leaf(fp) or fp, direct, child_summaries)

    signals = await _child_card_signals(tenant_id, direct)

    card: Dict[str, Any] = {
        "summary": summary,
        "topics": signals["topics"] or topics,
        "answers_questions": signals["answers_questions"],
        "doc_types": _doc_types(file_names),
        "date_range": _date_range(years),
        "key_entities": _candidate_entities(file_names, abstracts),
        "n_docs_direct": len(direct),
        "n_docs_total": n_docs_total,
        "n_subfolders": len(child_folders),
        # child_summaries[i] 는 child_folders[i] 에 대응 (위 루프에서 동순서로 append).
        "subfolders": [{"name": nm, "one_liner": s} for (nm, s) in child_summaries],
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
                    "doc_role": role,
                    "folder_path": fp,
                    "card": card,
                    "signature": signature,
                    "built_at": card["built_at"],
                },
                on_conflict="tenant_id,doc_role,folder_path",
            )
            .execute
        )
    except Exception as e:
        logger.warning("[folder_cards] upsert failed (%s): %s", fp, e)
        return {"folder_path": fp, "changed": False, "skipped": False, "signature": signature,
                "error": str(e)}

    logger.info(
        "[folder_cards] built %s (role=%s) direct=%d total=%d subfolders=%d topics=%d",
        fp, role, len(direct), n_docs_total, len(child_folders), len(topics),
    )
    return {"folder_path": fp, "changed": True, "skipped": False, "signature": signature}


async def _count_descendant_files(tenant_id: str, folder_path: str, doc_role: str) -> int:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref", count="exact")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", doc_role)
            .like("folder_path", f"{folder_path}/%")
            .limit(1)
            .execute
        )
        return int(getattr(resp, "count", 0) or 0)
    except Exception:
        return 0


async def rebuild_card_with_propagation(
    tenant_id: str,
    folder_path: str,
    doc_role: str = "content",
) -> int:
    """폴더 카드 빌드 후, 카드가 *바뀌면* 부모로 올라가며 재빌드. 안 바뀌면 중단.

    경로 깊이만큼만(보통 ~7) 도므로 증분 비용 bounded. 반환: 빌드된 폴더 수.
    """
    fp = _norm(folder_path)
    role = normalize_doc_role(doc_role)
    built = 0
    cur: Optional[str] = fp
    while cur:
        res = await build_folder_card(tenant_id, cur, role)
        built += 1
        if res.get("skipped") and not res.get("changed"):
            # 이 폴더 카드가 안 바뀜 → 상위도 안 바뀜 → 중단.
            break
        cur = _parent(cur)
    return built


async def _delete_card_row(tenant_id: str, folder_path: str, doc_role: str) -> None:
    """단일 폴더 카드 행 삭제 (정확히 그 folder_path)."""
    try:
        await asyncio.to_thread(
            supabase.table("knowledge_folder_cards")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("doc_role", doc_role)
            .eq("folder_path", folder_path)
            .execute
        )
    except Exception as e:
        logger.debug("[folder_cards] delete card row failed (%s): %s", folder_path, e)


async def delete_folder_cards(
    tenant_id: str, folder_path: str, doc_role: Optional[str] = None
) -> int:
    """폴더 + 그 하위(subtree)의 카드 행을 모두 삭제. 폴더 삭제 시 호출.

    doc_role 미지정이면 모든 role. 반환: 삭제 시도한 그룹 수(대략).
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
            if doc_role:
                base = base.eq("doc_role", normalize_doc_role(doc_role))
            await asyncio.to_thread(q(base).execute)
            n += 1
        except Exception as e:
            logger.warning("[folder_cards] delete_folder_cards failed (%s): %s", fp, e)
    logger.info("[folder_cards] deleted cards under %s (role=%s)", fp, doc_role or "(all)")
    return n


async def rebuild_folders(
    tenant_id: str, folder_paths: List[str], doc_role: str = "content"
) -> Dict[str, Any]:
    """주어진 폴더들 + 그 *조상 전부* 를 bottom-up(잎부터)으로 1회씩 재생성.

    업로드/파일삭제 배치 후 *영향받은 폴더만* 갱신하는 경로 (full backfill 대비 저렴, storm 없음).
    각 폴더는 signature-skip 으로 실제 바뀐 것만 LLM 호출. 같은 폴더는 한 번만 빌드(dedupe).
    """
    role = normalize_doc_role(doc_role)
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
        res = await build_folder_card(tenant_id, fp, role)
        if res.get("deleted"):
            deleted += 1
        elif res.get("changed"):
            built += 1
        else:
            skipped += 1
    logger.info(
        "[folder_cards] rebuild_folders role=%s affected=%d built=%d skipped=%d deleted=%d",
        role, len(affected), built, skipped, deleted,
    )
    return {"built": built, "skipped": skipped, "deleted": deleted}


async def backfill_tenant(tenant_id: str, doc_role: Optional[str] = None) -> Dict[str, Any]:
    """tenant 의 모든 폴더 카드를 bottom-up(잎부터)으로 1회 빌드. signature 동일이면 skip.

    doc_role 미지정 시 knowledge_files 에 등장한 모든 doc_role 에 대해 수행.
    """
    roles: List[str]
    if doc_role:
        roles = [normalize_doc_role(doc_role)]
    else:
        roles = await _distinct_doc_roles(tenant_id)

    total_built = 0
    total_skipped = 0
    for role in roles:
        folders = await _all_folders_for_tenant(tenant_id, role)
        # 깊은(잎) 폴더부터 → 부모가 자식 카드를 집계할 때 이미 존재.
        folders.sort(key=lambda p: p.count("/"), reverse=True)
        for fp in folders:
            res = await build_folder_card(tenant_id, fp, role)
            if res.get("changed"):
                total_built += 1
            else:
                total_skipped += 1
        logger.info("[folder_cards] backfill role=%s folders=%d", role, len(folders))

    return {"tenant_id": tenant_id, "roles": roles, "built": total_built, "skipped": total_skipped}


async def _distinct_doc_roles(tenant_id: str) -> List[str]:
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("doc_role")
            .eq("tenant_id", tenant_id)
            .limit(_FETCH_LIMIT)
            .execute
        )
        roles = {normalize_doc_role(r.get("doc_role")) for r in (resp.data or [])}
        return sorted(roles) or ["content"]
    except Exception:
        return ["content"]
