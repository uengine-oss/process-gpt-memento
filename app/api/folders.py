"""Folders 라우터 — 폴더 트리 네비게이션 전용 엔드포인트.

deepagents-lite 의 knowledge-navigator 서브에이전트가 *폴더 트리를 인덱스로 삼아*
질문에 답할 자료를 직접 찾아 내려가게 하는 경로. 대규모(~50GB) 코퍼스에서 8개 유사
사업이 거의 동일한 폴더 골격을 공유하므로, flat 임베딩 검색은 cross-contamination 이
심하다. 폴더 경로가 구별 신호다.

엔드포인트:
    GET /folders/tree   → 선택 루트들 아래 폴더 골격 + (있으면) 폴더카드 (얕게, 문서 dump X)
    GET /folders/open   → 한 폴더의 직속 자식(하위폴더 + 문서 abstract)

설계 메모:
- Progressive disclosure: tree 는 폴더 골격 + 카드/카운트만. 개별 파일은 dump 하지 않는다.
  에이전트가 유망 폴더를 골라 ``/folders/open`` 으로 직속 자식을 본다.
- folder card (knowledge_folder_cards 테이블) 는 Stage 2 산출물. 테이블이 없거나 비어 있으면
  ``card=null`` 로 graceful 동작 (Stage 1 은 폴더명 + 자식 문서 abstract 만으로 네비 가능).
- LLM 에 노출되는 식별자는 ``folder_path`` / ``file_name`` 만. file_id(긴 path/uuid)는 다루지 않음.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.core.supabase_client import supabase
from app.services.knowledge_files import compose_path

router = APIRouter()
logger = logging.getLogger(__name__)


# 한 번에 끌어올 knowledge_files row 상한 (tree 빌드용). 대규모 tenant 안전장치.
_TREE_FETCH_LIMIT = 20_000
# tree 노드당 sample_abstracts 최대 개수.
_SAMPLE_ABSTRACTS = 3
# tree 기본 깊이 (루트 기준 하위 단계 수).
_DEFAULT_TREE_DEPTH = 2
_MAX_TREE_DEPTH = 6


# ─────────────────────────────────────────────────────────────────────────────
# 경로 헬퍼
# ─────────────────────────────────────────────────────────────────────────────

def _norm(path: Optional[str]) -> str:
    return (path or "").strip().strip("/")


def _ancestors(path: str) -> List[str]:
    """``"a/b/c"`` → ``["a", "a/b", "a/b/c"]`` (자기 자신 포함)."""
    if not path:
        return []
    parts = [p for p in path.split("/") if p]
    out: List[str] = []
    acc: List[str] = []
    for p in parts:
        acc.append(p)
        out.append("/".join(acc))
    return out


def _parent(path: str) -> Optional[str]:
    if not path or "/" not in path:
        return None
    return path.rsplit("/", 1)[0]


def _leaf(path: str) -> str:
    return path.rsplit("/", 1)[-1] if path else ""


def _abstract_of(row: Dict[str, Any]) -> str:
    card = row.get("doc_card")
    if isinstance(card, dict):
        a = card.get("abstract")
        if isinstance(a, str) and a.strip():
            return a.strip()
    return ""


def _n_pages_of(row: Dict[str, Any]) -> Optional[int]:
    card = row.get("doc_card")
    if isinstance(card, dict):
        v = card.get("n_pages")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 폴더카드 조회 (Stage 2 테이블 — 없으면 graceful 빈 dict)
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_folder_cards(
    tenant_id: str, doc_role: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """``knowledge_folder_cards`` 에서 {folder_path: card} 반환.

    테이블이 아직 없거나(Stage 1) 조회 실패면 빈 dict. 호출부는 ``.get(fp)`` 로 None 허용.
    """
    try:
        q = (
            supabase.table("knowledge_folder_cards")
            .select("folder_path, card")
            .eq("tenant_id", tenant_id)
        )
        if doc_role:
            q = q.eq("doc_role", doc_role)
        resp = await asyncio.to_thread(q.execute)
        out: Dict[str, Dict[str, Any]] = {}
        for r in (resp.data or []):
            fp = _norm(r.get("folder_path"))
            card = r.get("card")
            if fp and isinstance(card, dict):
                out[fp] = card
        return out
    except Exception as e:
        # Stage 1: 테이블 미존재 등 → 조용히 빈 카드.
        logger.debug("[/folders] folder_cards unavailable (ok in Stage 1): %s", e)
        return {}


def _card_brief(card: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """tree/open 응답에 실을 폴더카드 요약(요약문 + 토픽)."""
    if not isinstance(card, dict):
        return None
    return {
        "summary": card.get("summary") or "",
        "topics": card.get("topics") or [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /folders/tree
# ─────────────────────────────────────────────────────────────────────────────

# source_ref IN 배치 크기 — 폴더 통째 선택 시 file_id 가 수천 개일 수 있어
# GET URL 길이 / PostgREST 한도를 넘지 않게 나눠 조회한다.
_IN_CHUNK = 150


async def _fetch_kf_by_refs(
    tenant_id: str,
    select_cols: str,
    refs: List[str],
    *,
    doc_role: Optional[str] = None,
    folder_eq: Optional[str] = None,
    folder_like: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """source_ref 화이트리스트로 knowledge_files 조회 (IN 청크 배치).

    ★ 보안 경계 — refs(사용자가 선택한 file_id=source_ref) 밖의 자료는 절대 포함되지 않는다.
    refs 가 크면 _IN_CHUNK 단위로 나눠 여러 번 조회 후 합친다.
    """
    refs = [str(x) for x in refs if x]
    if not refs:
        return []
    out: List[Dict[str, Any]] = []
    for i in range(0, len(refs), _IN_CHUNK):
        chunk = refs[i:i + _IN_CHUNK]
        q = (
            supabase.table("knowledge_files")
            .select(select_cols)
            .eq("tenant_id", tenant_id)
            .in_("source_ref", chunk)
        )
        if doc_role:
            q = q.eq("doc_role", doc_role)
        if folder_eq is not None:
            q = q.eq("folder_path", folder_eq)
        if folder_like is not None:
            q = q.like("folder_path", folder_like)
        if limit is not None:
            q = q.limit(limit)
        r = await asyncio.to_thread(q.execute)
        out.extend(r.data or [])
    return out


@router.get("/folders/tree")
async def folders_tree(
    tenant_id: str,
    roots: Optional[List[str]] = Query(default=None),
    file_ids: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
    doc_role: Optional[str] = Query(default=None),
    depth: int = Query(default=_DEFAULT_TREE_DEPTH, ge=1, le=_MAX_TREE_DEPTH),
):
    """선택 루트들 아래의 폴더 골격(+폴더카드, +자식 문서 abstract 샘플)을 반환.

    Args:
        tenant_id: 필수.
        roots: 선택된 folder_path 들(옵션). 비면 tenant 전체 최상위부터.
        doc_role: 옵션. 주면 그 role 자료만 집계.
        depth: 루트 기준 하위 몇 단계까지 펼칠지(기본 2). 더 깊은 곳은 ``/folders/open`` 으로.

    Returns:
        ``{"tree": [node, ...], "truncated": bool}``
        node = {folder_path, name, n_docs_direct, n_docs_total, n_subfolders,
                card, sample_abstracts, children}
    """
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    # ★ 보안 경계 — file_ids(=선택 source_ref) 또는 folder_paths(폴더 스코프) 안에서만 트리를 짠다.
    #   - file_ids: 개별 파일 선택. 화이트리스트 IN(청크).
    #   - folder_paths: 폴더째 선택. file_id 수천 개를 URL 로 안 넘기고 폴더 경로로 직접 조회(스케일).
    #   - 둘 다 없으면 레거시/직접호출 하위호환으로 tenant 전체.
    allow = [str(x) for x in (file_ids or []) if x]
    scope_folders = [str(x) for x in (folder_paths or []) if x and str(x).strip().strip("/")]
    _COLS = "source_ref, file_name, folder_path, doc_card, doc_role"
    try:
        # 개별 file_ids(allow) 와 폴더 스코프(scope_folders)를 union. 단일 소스면 예전과 동일 결과.
        rows = []
        _seen: set = set()

        def _add_tree_rows(new_rows):
            for r in (new_rows or []):
                key = (str(r.get("folder_path") or ""), str(r.get("file_name") or ""))
                if key in _seen:
                    continue
                _seen.add(key)
                rows.append(r)

        if allow:
            _add_tree_rows(await _fetch_kf_by_refs(tenant_id, _COLS, allow, doc_role=doc_role))
        if scope_folders:
            from app.services.knowledge_files import fetch_rows_by_folders
            _add_tree_rows(await fetch_rows_by_folders(
                tenant_id, _COLS, scope_folders, doc_role=doc_role, limit=_TREE_FETCH_LIMIT,
            ))
        if not allow and not scope_folders:
            q = (
                supabase.table("knowledge_files")
                .select(_COLS)
                .eq("tenant_id", tenant_id)
            )
            if doc_role:
                q = q.eq("doc_role", doc_role)
            base = (await asyncio.to_thread(q.limit(_TREE_FETCH_LIMIT).execute)).data or []
            # 채팅 첨부(folder_path="")가 스코프 없는 전체 트리의 루트에 뜨지 않게 제외.
            from app.services.knowledge_files import _is_chat_attachment_ref
            _add_tree_rows([r for r in base if not _is_chat_attachment_ref(r.get("source_ref"))])
    except Exception as e:
        logger.exception("[/folders/tree] query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    truncated = (not allow and not scope_folders) and len(rows) >= _TREE_FETCH_LIMIT

    # 집계 구조 빌드
    files_by_folder: Dict[str, List[Dict[str, Any]]] = {}
    all_folders: set[str] = set()
    direct: Dict[str, int] = {}
    total: Dict[str, int] = {}
    children: Dict[str, set[str]] = {}

    for r in rows:
        fp = _norm(r.get("folder_path"))
        if not fp:
            continue  # 루트 직속(폴더 없는) 파일은 트리 네비 대상 아님
        files_by_folder.setdefault(fp, []).append(r)
        direct[fp] = direct.get(fp, 0) + 1
        for anc in _ancestors(fp):
            all_folders.add(anc)
            total[anc] = total.get(anc, 0) + 1

    for fp in all_folders:
        par = _parent(fp)
        if par is not None:
            children.setdefault(par, set()).add(fp)

    cards = await _fetch_folder_cards(tenant_id, doc_role)

    # 시작 루트 결정
    norm_roots = [_norm(r) for r in (roots or []) if _norm(r)]
    start: List[str]
    if norm_roots:
        start = sorted({r for r in norm_roots if r in all_folders})
        if not start:
            # 선택 루트에 파일이 없으면(혹은 정확히 안 맞으면) 전체 최상위로 폴백
            start = sorted({f for f in all_folders if _parent(f) is None})
    else:
        start = sorted({f for f in all_folders if _parent(f) is None})

    def _node(fp: str, remaining: int) -> Dict[str, Any]:
        samples: List[str] = []
        for fr in files_by_folder.get(fp, []):
            ab = _abstract_of(fr)
            if ab:
                samples.append(f"{fr.get('file_name')}: {ab}")
            if len(samples) >= _SAMPLE_ABSTRACTS:
                break
        kids: List[Dict[str, Any]] = []
        if remaining > 0:
            for cfp in sorted(children.get(fp, set())):
                kids.append(_node(cfp, remaining - 1))
        return {
            "folder_path": fp,
            "name": _leaf(fp),
            "n_docs_direct": direct.get(fp, 0),
            "n_docs_total": total.get(fp, 0),
            "n_subfolders": len(children.get(fp, set())),
            "card": _card_brief(cards.get(fp)),
            "sample_abstracts": samples,
            "children": kids,
        }

    tree = [_node(fp, depth - 1) for fp in start]
    logger.info(
        "[/folders/tree] tenant=%s roots=%d doc_role=%s files=%d folders=%d start=%d depth=%d",
        tenant_id, len(norm_roots), doc_role, len(rows), len(all_folders), len(start), depth,
    )
    return {"tree": tree, "truncated": truncated}


# ─────────────────────────────────────────────────────────────────────────────
# GET /folders/card  — 단일 폴더 카드(요약/토픽/엔티티/메타). 폴더 선택 시 요약 패널용.
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/folders/card")
async def folder_card(
    tenant_id: str,
    folder_path: str,
    doc_role: Optional[str] = Query(default=None),
):
    """단일 폴더의 카드를 반환. 카드가 아직 없으면 ``card=null`` (graceful)."""
    if not tenant_id or not folder_path:
        raise HTTPException(status_code=400, detail="tenant_id, folder_path required")
    fp = _norm(folder_path)
    role = (doc_role or "content").strip().lower() or "content"
    try:
        resp = await asyncio.to_thread(
            supabase.table("knowledge_folder_cards")
            .select("card, built_at")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", role)
            .eq("folder_path", fp)
            .limit(1)
            .execute
        )
        rows = resp.data or []
    except Exception as e:
        logger.debug("[/folders/card] unavailable (table missing?): %s", e)
        return {"folder_path": fp, "card": None}
    card = rows[0].get("card") if rows else None
    built_at = rows[0].get("built_at") if rows else None
    return {"folder_path": fp, "card": card if isinstance(card, dict) else None, "built_at": built_at}


# ─────────────────────────────────────────────────────────────────────────────
# GET /folders/open
# ─────────────────────────────────────────────────────────────────────────────

# 직속 문서가 이 수를 넘으면 나열을 포기하고 query 모드로 전환한다.
# 임계는 "출력 줄 수"의 프록시 — 문서 한 줄 ≈ 210자라 200줄 ≈ 4.2만자이고, 이는 호출측
# (deepagents) 의 tool-result eviction 임계 8만자의 절반이다.
_OPEN_LIST_THRESHOLD = 200
# query 모드에서 최종적으로 남길 문서 수.
_OPEN_QUERY_LIMIT = 150
# 하위폴더 줄에도 같은 상한 — 직속 문서 0개인데 하위폴더가 수백 개인 폴더 방어.
_OPEN_MAX_SUBFOLDERS = 60
# 문서 N개를 채우려면 청크는 그보다 넉넉히 뽑아야 한다(한 문서가 청크 여러 개를 차지).
_CHUNK_FANOUT = 4
_CHUNK_FANOUT_RETRY = 10
_MAX_CHUNK_TOP_K = 2000
# query 모드에서 랭킹 후보로 훑을 직속 문서 수 상한 (source_ref 한 컬럼만 읽음).
_MAX_DIRECT_SCAN = 20_000

_DOC_COLS = "source_ref, file_name, folder_path, path, doc_card, doc_role, mime_type"


def _empty_open(fp: str) -> Dict[str, Any]:
    """스코프 밖 폴더 응답 — 호출측이 키 유무를 분기하지 않게 전체 스키마를 채운다."""
    return {
        "folder_path": fp, "subfolders": [], "docs": [],
        "n_docs_direct": 0, "n_subfolders_total": 0,
        "overflow": False, "query": "", "role_counts": {}, "card": None,
    }


async def _direct_docs_query(
    tenant_id: str,
    fp: str,
    *,
    cols: str,
    doc_role: Optional[str],
    allow: List[str],
    scope_folders: List[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """이 폴더 *직속* 문서 행 조회. allow(개별 선택) 모드와 폴더 스코프 모드 공통 진입점."""
    if allow and not scope_folders:
        # _fetch_kf_by_refs 의 limit 은 IN 배치(150개)마다 걸리므로 합계는 limit 을 넘을 수 있다.
        # 여기서 최종 절단해 두 모드의 반환 크기 계약을 같게 맞춘다.
        rows = await _fetch_kf_by_refs(
            tenant_id, cols, allow, doc_role=doc_role, folder_eq=fp, limit=limit,
        )
        return rows[:limit]
    q = (
        supabase.table("knowledge_files")
        .select(cols)
        .eq("tenant_id", tenant_id)
        .eq("folder_path", fp)
    )
    if doc_role:
        q = q.eq("doc_role", doc_role)
    return (await asyncio.to_thread(q.limit(limit).execute)).data or []


async def _count_direct_docs(
    tenant_id: str,
    fp: str,
    *,
    doc_role: Optional[str],
    allow: List[str],
    scope_folders: List[str],
) -> int:
    """직속 문서 정확 개수. 행을 끌어오지 않고 count 만 (임계 초과 시에만 호출)."""
    if allow and not scope_folders:
        rows = await _fetch_kf_by_refs(
            tenant_id, "source_ref", allow, doc_role=doc_role, folder_eq=fp,
        )
        return len(rows)
    try:
        q = (
            supabase.table("knowledge_files")
            .select("source_ref", count="exact")
            .eq("tenant_id", tenant_id)
            .eq("folder_path", fp)
        )
        if doc_role:
            q = q.eq("doc_role", doc_role)
        r = await asyncio.to_thread(q.limit(1).execute)
        return int(getattr(r, "count", 0) or 0)
    except Exception as e:
        logger.warning("[/folders/open] count failed (%s): %s", fp, e)
        return 0


async def _rank_direct_refs(
    tenant_id: str,
    query: str,
    candidate_refs: List[str],
    limit: int,
) -> List[str]:
    """직속 문서들을 query 관련도 순으로 정렬해 상위 ``limit`` 개의 source_ref 반환.

    청크 임베딩 검색 → file_id 롤업(문서당 최초 히트 = 최고 순위). 1차에서 limit 을 못
    채우면 top_k 를 키워 한 번 더 — 소수 문서가 상위 청크를 독점하면 문서 수가 안 나온다.
    """
    if not candidate_refs or not query.strip():
        return []
    from app.services.vector_store import get_vector_store

    vsm = get_vector_store()
    allowed = set(candidate_refs)
    ordered: List[str] = []
    for fanout in (_CHUNK_FANOUT, _CHUNK_FANOUT_RETRY):
        top_k = min(_MAX_CHUNK_TOP_K, max(limit, limit * fanout))
        metas = await vsm.search_chunk_metadata(
            query,
            {"tenant_id": tenant_id, "file_id": candidate_refs},
            top_k=top_k,
        )
        seen: set = set()
        ordered = []
        for m in metas:
            ref = str(m.get("file_id") or "")
            if not ref or ref in seen or ref not in allowed:
                continue
            seen.add(ref)
            ordered.append(ref)
        logger.info(
            "[/folders/open] rank q=%r candidates=%d top_k=%d → chunks=%d docs=%d",
            query[:60], len(candidate_refs), top_k, len(metas), len(ordered),
        )
        if len(ordered) >= limit or top_k >= _MAX_CHUNK_TOP_K:
            break
    return ordered[:limit]


async def _direct_role_counts(
    tenant_id: str,
    fp: str,
    *,
    doc_role: Optional[str],
    allow: List[str],
    scope_folders: List[str],
) -> Dict[str, int]:
    """직속 문서의 doc_role 분포. 힌트용이라 doc_role 한 컬럼만 읽는다(abstract 제외)."""
    rows = await _direct_docs_query(
        tenant_id, fp, cols="doc_role", doc_role=doc_role,
        allow=allow, scope_folders=scope_folders, limit=_MAX_DIRECT_SCAN,
    )
    out: Dict[str, int] = {}
    for r in rows:
        role = (r.get("doc_role") or "content").strip() or "content"
        out[role] = out.get(role, 0) + 1
    return out


@router.get("/folders/open")
async def folders_open(
    tenant_id: str,
    folder_path: str,
    file_ids: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
    doc_role: Optional[str] = Query(default=None),
    query: Optional[str] = Query(default=None),
    limit: int = Query(default=_OPEN_QUERY_LIMIT, ge=1, le=500),
    list_threshold: int = Query(default=_OPEN_LIST_THRESHOLD, ge=1, le=2000),
):
    """한 폴더의 직속 자식 — 하위폴더(카드 요약) + 문서(abstract) — 반환.

    출력 크기는 폴더 크기가 아니라 파라미터가 정한다. 직속 문서가 ``list_threshold`` 를
    넘으면 나열하지 않고, ``query`` 가 오면 그 질의 관련 상위 ``limit`` 개만 돌려준다.
    query 가 없으면 문서 목록 대신 좁힐 단서(하위폴더/역할분포/폴더카드)만 준다.

    Args:
        tenant_id: 필수.
        folder_path: 펼칠 폴더 경로.
        doc_role: 옵션.
        query: 이 폴더 안에서 찾는 내용. 임계 초과 시 이걸로 문서를 추린다.
        limit: query 모드에서 남길 문서 수.
        list_threshold: 이 수를 넘으면 나열 대신 query 모드.

    Returns:
        ``{"folder_path", "subfolders", "docs", "n_docs_direct", "n_subfolders_total",
           "overflow", "query", "role_counts", "card"}``
        subfolders = {folder_path, name, n_docs_total, card}
        docs = {file_name, folder_path, path, abstract, doc_role, n_pages, mime_type}
    """
    if not tenant_id or not folder_path:
        raise HTTPException(status_code=400, detail="tenant_id, folder_path required")

    fp = _norm(folder_path)
    if not fp:
        raise HTTPException(status_code=400, detail="folder_path empty")

    # ★ 보안 경계 — file_ids(개별 선택) 또는 folder_paths(폴더 스코프) 안에서만 노출.
    #   folder_paths 가 오면 여는 폴더(fp)가 그 subtree 안인지 순수 문자열로 검증(refs 열거 불필요).
    allow = [str(x) for x in (file_ids or []) if x]
    scope_folders = [_norm(x) for x in (folder_paths or []) if _norm(x)]
    if scope_folders and not any(fp == s or fp.startswith(s + "/") for s in scope_folders):
        return _empty_open(fp)

    q = (query or "").strip()

    # ── 직속 문서 ──
    #   폴더 스코프 안을 여는 중이면(scope_folders) 폴더 직속 문서 *전부*. 개별 file_ids 모드
    #   (폴더 스코프 없음)일 때만 그 file_ids 로 좁힌다 → 폴더+파일 공존 시 폴더 문서가 사라지지 않게.
    #
    #   ★ 크기 가드: 먼저 threshold+1 개만 읽는다. 임계 이하면 그 행이 곧 전체라 추가 쿼리가
    #     없고, 초과면 무거운 doc_card 를 2000행씩 끌어오는 일 자체가 일어나지 않는다.
    role_counts: Dict[str, int] = {}
    try:
        probe_rows = await _direct_docs_query(
            tenant_id, fp, cols=_DOC_COLS, doc_role=doc_role,
            allow=allow, scope_folders=scope_folders, limit=list_threshold + 1,
        )
        overflow = len(probe_rows) > list_threshold
        if not overflow:
            direct_rows = probe_rows
            n_docs_direct = len(probe_rows)
        else:
            n_docs_direct = await _count_direct_docs(
                tenant_id, fp, doc_role=doc_role, allow=allow, scope_folders=scope_folders,
            )
            if not q:
                # 목록을 줄 수 없는 상태 — 대신 좁힐 단서(역할분포)를 준다.
                direct_rows = []
                role_counts = await _direct_role_counts(
                    tenant_id, fp, doc_role=doc_role,
                    allow=allow, scope_folders=scope_folders,
                )
            else:
                # 랭킹 후보는 source_ref 한 컬럼만(가벼움) → 상위 limit 개만 전체 컬럼으로 재조회.
                ref_rows = await _direct_docs_query(
                    tenant_id, fp, cols="source_ref", doc_role=doc_role,
                    allow=allow, scope_folders=scope_folders, limit=_MAX_DIRECT_SCAN,
                )
                candidates = [str(r.get("source_ref")) for r in ref_rows if r.get("source_ref")]
                top_refs = await _rank_direct_refs(tenant_id, q, candidates, limit)
                by_ref = {
                    str(r.get("source_ref")): r
                    for r in await _fetch_kf_by_refs(
                        tenant_id, _DOC_COLS, top_refs, doc_role=doc_role, folder_eq=fp,
                    )
                    if r.get("source_ref")
                }
                direct_rows = [by_ref[ref] for ref in top_refs if ref in by_ref]
    except Exception as e:
        logger.exception("[/folders/open] direct query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    # 하위 (descendant) — 하위폴더 집계용
    try:
        if allow and not scope_folders:
            desc_rows = await _fetch_kf_by_refs(
                tenant_id, "folder_path", allow, doc_role=doc_role, folder_like=f"{fp}/%",
            )
        else:
            cq = (
                supabase.table("knowledge_files")
                .select("folder_path")
                .eq("tenant_id", tenant_id)
                .like("folder_path", f"{fp}/%")
            )
            if doc_role:
                cq = cq.eq("doc_role", doc_role)
            desc_rows = (await asyncio.to_thread(cq.limit(_TREE_FETCH_LIMIT).execute)).data or []
    except Exception as e:
        logger.exception("[/folders/open] descendant query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    # 직속 하위폴더 = fp 다음 한 세그먼트. total = 그 subtree 의 파일 수.
    prefix = fp + "/"
    subfolder_total: Dict[str, int] = {}
    for r in desc_rows:
        dpath = _norm(r.get("folder_path"))
        if not dpath.startswith(prefix):
            continue
        rest = dpath[len(prefix):]
        first_seg = rest.split("/", 1)[0]
        child_fp = prefix + first_seg
        subfolder_total[child_fp] = subfolder_total.get(child_fp, 0) + 1

    cards = await _fetch_folder_cards(tenant_id, doc_role)

    # 하위폴더는 문서 수 많은 순으로 상한만큼 — 수백 개짜리 폴더에서 하위폴더 줄이 폭주하지 않게.
    n_subfolders_total = len(subfolder_total)
    top_subfolders = sorted(subfolder_total.items(), key=lambda kv: (-kv[1], kv[0]))
    subfolders = [
        {
            "folder_path": cfp,
            "name": _leaf(cfp),
            "n_docs_total": cnt,
            "card": _card_brief(cards.get(cfp)),
        }
        for cfp, cnt in top_subfolders[:_OPEN_MAX_SUBFOLDERS]
    ]

    docs = [
        {
            "file_name": r.get("file_name"),
            "folder_path": _norm(r.get("folder_path")),
            "path": r.get("path") or compose_path(r.get("folder_path"), r.get("file_name")),
            "abstract": _abstract_of(r),
            "doc_role": r.get("doc_role") or "content",
            "n_pages": _n_pages_of(r),
            "mime_type": r.get("mime_type"),
        }
        for r in direct_rows
    ]

    logger.info(
        "[/folders/open] tenant=%s folder=%s q=%r → subfolders=%d/%d docs=%d/%d overflow=%s",
        tenant_id, fp, q[:60], len(subfolders), n_subfolders_total,
        len(docs), n_docs_direct, overflow,
    )
    return {
        "folder_path": fp,
        "subfolders": subfolders,
        "docs": docs,
        "n_docs_direct": n_docs_direct,
        "n_subfolders_total": n_subfolders_total,
        "overflow": overflow,
        "query": q,
        "role_counts": role_counts,
        "card": _card_brief(cards.get(fp)),
    }
