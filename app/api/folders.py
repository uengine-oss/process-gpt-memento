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

@router.get("/folders/tree")
async def folders_tree(
    tenant_id: str,
    roots: Optional[List[str]] = Query(default=None),
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

    try:
        q = (
            supabase.table("knowledge_files")
            .select("file_name, folder_path, doc_card, doc_role")
            .eq("tenant_id", tenant_id)
        )
        if doc_role:
            q = q.eq("doc_role", doc_role)
        resp = await asyncio.to_thread(q.limit(_TREE_FETCH_LIMIT).execute)
        rows = resp.data or []
    except Exception as e:
        logger.exception("[/folders/tree] query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    truncated = len(rows) >= _TREE_FETCH_LIMIT

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

@router.get("/folders/open")
async def folders_open(
    tenant_id: str,
    folder_path: str,
    doc_role: Optional[str] = Query(default=None),
):
    """한 폴더의 직속 자식 — 하위폴더(카드 요약) + 문서(abstract) — 반환.

    Args:
        tenant_id: 필수.
        folder_path: 펼칠 폴더 경로.
        doc_role: 옵션.

    Returns:
        ``{"folder_path", "subfolders": [...], "docs": [...]}``
        subfolders = {folder_path, name, n_docs_total, card}
        docs = {file_name, folder_path, abstract, doc_role, n_pages, mime_type}
    """
    if not tenant_id or not folder_path:
        raise HTTPException(status_code=400, detail="tenant_id, folder_path required")

    fp = _norm(folder_path)
    if not fp:
        raise HTTPException(status_code=400, detail="folder_path empty")

    # 직속 문서 (정확히 이 폴더)
    try:
        eq = (
            supabase.table("knowledge_files")
            .select("file_name, folder_path, path, doc_card, doc_role, mime_type")
            .eq("tenant_id", tenant_id)
            .eq("folder_path", fp)
        )
        if doc_role:
            eq = eq.eq("doc_role", doc_role)
        direct_rows = (await asyncio.to_thread(eq.execute)).data or []
    except Exception as e:
        logger.exception("[/folders/open] direct query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    # 하위 (descendant) — 하위폴더 집계용
    try:
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

    subfolders = [
        {
            "folder_path": cfp,
            "name": _leaf(cfp),
            "n_docs_total": cnt,
            "card": _card_brief(cards.get(cfp)),
        }
        for cfp, cnt in sorted(subfolder_total.items())
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
        "[/folders/open] tenant=%s folder=%s → subfolders=%d docs=%d",
        tenant_id, fp, len(subfolders), len(docs),
    )
    return {"folder_path": fp, "subfolders": subfolders, "docs": docs}
