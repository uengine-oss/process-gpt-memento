"""Navigator 라우터 — agent navigation 전용 엔드포인트.

deep-agents-temp 의 list_documents / grep_in_document / read_document_page 도구가
사용하는 3개 엔드포인트. 기존 /retrieve 는 다른 곳에서도 쓰니까 안 건드림 — 이건
별도 신설(=/search 와 같은 정책).

엔드포인트:
    GET /catalog           → 선택 자료의 doc_card 목록
    GET /document/grep     → 한 문서 안에서 정확 토큰 위치 찾기 (regex 옵션)
    GET /document/page     → 페이지 범위 본문 가져오기

매칭 키:
    LLM 에 노출되는 식별자는 ``file_name`` 만. 내부에서 ``(tenant_id, file_name)`` →
    ``knowledge_files.source_ref`` 해석. file_id(긴 uuid/path)는 LLM 이 다루지 않음
    (긴 식별자 손글씨 베끼다 망가뜨리는 버그 클래스 차단).
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from app.core.supabase_client import supabase
from app.services.knowledge_files import compose_path

router = APIRouter()
logger = logging.getLogger(__name__)


def _norm_folder(p: Optional[str]) -> str:
    """폴더 경로 정규화 — 앞뒤 슬래시/공백 제거. 폴더 스코프 접두어 검사용."""
    return (p or "").strip().strip("/")


# ─────────────────────────────────────────────────────────────────────────────
# 헬퍼 — file_name → file_id (source_ref) 해석
# ─────────────────────────────────────────────────────────────────────────────

async def _resolve_file_id(
    tenant_id: str,
    *,
    path: Optional[str] = None,
    file_name: Optional[str] = None,
    folder_path: Optional[str] = None,
) -> Optional[str]:
    """문서 → knowledge_files.source_ref 해석.

    우선순위(견고한 표준 경로):
    1) ``path`` (전체 상대경로 핸들) **정확 매칭**. 에이전트가 도구 출력의 path 를 *그대로 복사*해
       넘기는 경로. 동명 파일도 path 가 사업별로 유일하므로 자동 구별 — 재조합 슬립 없음.
    2) path 정확 매칭 실패 시: path 의 **basename 으로 file_name 매칭**(most-recent) — 모델이 path 를
       살짝 틀리거나 레거시 행(path NULL)일 때의 폴백(이중 방어).
    3) path 없이 ``file_name`` (+옵션 folder_path) — 레거시/`/document/raw` 호환.
    다중이면 modified_time desc 로 가장 최근.
    """
    def _ref(rows):
        return rows[0].get("source_ref") if rows else None

    try:
        p = (path or "").strip().strip("/")
        if p:
            r = await asyncio.to_thread(
                supabase.table("knowledge_files")
                .select("source_ref, modified_time")
                .eq("tenant_id", tenant_id).eq("path", p)
                .order("modified_time", desc=True).limit(1).execute
            )
            ref = _ref(r.data or [])
            if ref:
                return ref
            basename = p.rsplit("/", 1)[-1]
            if basename:
                r2 = await asyncio.to_thread(
                    supabase.table("knowledge_files")
                    .select("source_ref, modified_time")
                    .eq("tenant_id", tenant_id).eq("file_name", basename)
                    .order("modified_time", desc=True).limit(1).execute
                )
                return _ref(r2.data or [])
            return None

        if file_name:
            q = (
                supabase.table("knowledge_files")
                .select("source_ref, modified_time")
                .eq("tenant_id", tenant_id).eq("file_name", file_name)
            )
            if folder_path is not None and str(folder_path).strip() != "":
                q = q.eq("folder_path", str(folder_path).strip().strip("/"))
            r = await asyncio.to_thread(q.order("modified_time", desc=True).limit(1).execute)
            return _ref(r.data or [])
        return None
    except Exception as e:
        logger.warning(
            "[navigator] resolve failed (tenant=%s path=%s name=%s): %s",
            tenant_id, path, file_name, e,
        )
        return None


# ─────────────────────────────────────────────────────────────────────────────
# GET /catalog
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/catalog")
async def catalog(
    tenant_id: str,
    file_ids: Optional[List[str]] = Query(default=None),
    file_names: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
):
    """선택 자료의 doc_card 목록 반환.

    Args:
        tenant_id: 필수.
        file_ids: knowledge_files.source_ref 리스트(옵션). 지정하면 그 파일만.
        file_names: knowledge_files.file_name 리스트(옵션). LLM 도구가 보통 이 경로로 사용.
            두 파라미터 동시에 사용하면 둘 다 매칭(OR)이 아니라 file_ids 가 우선.
            둘 다 비면 tenant 전체 카탈로그.

    Returns:
        ``{"response": [{file_id, file_name, doc_card, ...}, ...]}``
    """
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    _CATALOG_COLS = (
        "source_ref, source_type, file_name, folder_path, path, mime_type, "
        "size_bytes, modified_time, indexed_at, index_status, doc_card, doc_role"
    )
    try:
        cleaned_ids = [str(x) for x in (file_ids or []) if x]
        cleaned_names = [str(x) for x in (file_names or []) if x]
        cleaned_folders = [str(x) for x in (folder_paths or []) if x and str(x).strip().strip("/")]
        # 개별(file_ids/names) 과 폴더 스코프를 *union* 으로 합친다(공존 스코프: 폴더 + 방 첨부 등).
        # 단일 소스면 각 분기 결과가 예전과 동일 → 기존 호출 영향 없음. 둘 다면 합쳐서 dedup.
        rows: list = []
        seen_refs: set = set()

        def _add_rows(new_rows):
            for r in (new_rows or []):
                ref = r.get("source_ref")
                if ref in seen_refs:
                    continue
                seen_refs.add(ref)
                rows.append(r)

        if cleaned_ids or cleaned_names:
            query = (
                supabase.table("knowledge_files")
                .select(_CATALOG_COLS)
                .eq("tenant_id", tenant_id)
            )
            if cleaned_ids:
                query = query.in_("source_ref", cleaned_ids)
            elif cleaned_names:
                query = query.in_("file_name", cleaned_names)
            response = await asyncio.to_thread(query.order("file_name", desc=False).execute)
            _add_rows(response.data or [])
        if cleaned_folders:
            from app.services.knowledge_files import fetch_rows_by_folders
            _add_rows(await fetch_rows_by_folders(tenant_id, _CATALOG_COLS, cleaned_folders))
        if not cleaned_ids and not cleaned_names and not cleaned_folders:
            # 스코프 없음 → tenant 전체. 채팅 첨부는 전체조회에 안 섞이게 제외.
            from app.services.knowledge_files import _is_chat_attachment_ref
            response = await asyncio.to_thread(
                supabase.table("knowledge_files").select(_CATALOG_COLS)
                .eq("tenant_id", tenant_id).order("file_name", desc=False).execute
            )
            _add_rows([r for r in (response.data or []) if not _is_chat_attachment_ref(r.get("source_ref"))])
        rows = sorted(rows, key=lambda r: (r.get("file_name") or ""))

        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "file_id": r.get("source_ref"),
                "file_name": r.get("file_name"),
                "folder_path": r.get("folder_path") or "",
                "path": r.get("path") or compose_path(r.get("folder_path"), r.get("file_name")),
                "mime_type": r.get("mime_type"),
                "size_bytes": r.get("size_bytes"),
                "modified_time": r.get("modified_time"),
                "indexed_at": r.get("indexed_at"),
                "index_status": r.get("index_status"),
                "source_type": r.get("source_type"),
                "doc_card": r.get("doc_card"),
                "doc_role": r.get("doc_role") or "content",
            })
        logger.info(
            "[/catalog] tenant=%s ids=%d names=%d → %d cards",
            tenant_id, len(cleaned_ids), len(cleaned_names), len(out),
        )
        return {"response": out}

    except Exception as e:
        logger.exception("[/catalog] failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# GET /glossary/inline
#
# 선택된 file_ids 중 doc_role='glossary' 인 자료들의 본문(페이지 전체)을 모아
# 반환한다. deep-agents-temp 의 채팅 진입점에서 호출 — 사용자 메시지에
# ``[용어 사전 — 자동 첨부]`` 섹션으로 prepend 해서, 모든 sub 가 일관된
# 용어 매핑을 보게 한다.
# ─────────────────────────────────────────────────────────────────────────────

# 컨텍스트 폭주 방지를 위한 상한 (문자 단위, 대략 토큰의 4배)
_GLOSSARY_INLINE_MAX_CHARS = 32_000


@router.get("/glossary/inline")
async def glossary_inline(
    tenant_id: str,
    file_ids: Optional[List[str]] = Query(default=None),
    max_chars: int = Query(default=_GLOSSARY_INLINE_MAX_CHARS, ge=1_000, le=200_000),
):
    """선택된 file_ids 중 ``doc_role='glossary'`` 인 자료의 본문을 페이지 순으로 합쳐 반환.

    Args:
        tenant_id: 필수.
        file_ids: knowledge_files.source_ref 리스트. *반드시 사용자가 선택한 파일* 만 넘긴다.
        max_chars: 합쳐진 본문 길이 상한. 초과 시 truncate 표시 후 잘림.

    Returns:
        ``{"response": [{file_name, file_id, content, n_pages, truncated}, ...],
          "total_chars": int, "truncated": bool}``
    """
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    cleaned_ids = [str(x) for x in (file_ids or []) if x]
    if not cleaned_ids:
        return {"response": [], "total_chars": 0, "truncated": False}

    try:
        # ★ 우선순위: knowledge_files.glossary_compact (정제본 컬럼) > 페이지 합본 (fallback).
        # 정제본은 ingest 시 LLM 추출로 만들어진 형식-자유 마크다운 → 토큰 크게 절약.
        rows_resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref, file_name, glossary_compact, doc_card")
            .eq("tenant_id", tenant_id)
            .eq("doc_role", "glossary")
            .in_("source_ref", cleaned_ids)
            .execute
        )
        glossary_rows = rows_resp.data or []
        if not glossary_rows:
            return {"response": [], "total_chars": 0, "truncated": False}

        out: List[Dict[str, Any]] = []
        total_chars = 0
        global_truncated = False

        for row in glossary_rows:
            file_id = row.get("source_ref") or ""
            if not file_id:
                continue
            file_name = row.get("file_name") or ""
            compact = row.get("glossary_compact")
            card = row.get("doc_card") if isinstance(row.get("doc_card"), dict) else {}

            content: str
            source: str
            n_pages: int

            if isinstance(compact, str) and compact.strip():
                # 정제본 사용
                content = compact.strip()
                source = "compact"
                n_pages = int(card.get("n_pages") or 0) if card else 0
            else:
                # fallback: 페이지 본문 합본
                page_resp = await asyncio.to_thread(
                    supabase.table("document_pages")
                    .select("page_number, content")
                    .eq("tenant_id", tenant_id)
                    .eq("file_id", file_id)
                    .order("page_number", desc=False)
                    .execute
                )
                pages = page_resp.data or []
                text_parts: List[str] = []
                for p in pages:
                    t = (p.get("content") or "").strip()
                    if t:
                        text_parts.append(t)
                content = "\n\n".join(text_parts)
                source = "raw_pages"
                n_pages = len(pages)

            # max_chars truncate (정제본·raw 공통)
            file_truncated = False
            remaining = max_chars - total_chars
            if remaining <= 0:
                file_truncated = True
                content = ""
                global_truncated = True
            elif len(content) > remaining:
                content = content[:remaining] + "\n…(truncated)"
                file_truncated = True
                global_truncated = True

            total_chars += len(content)
            out.append({
                "file_name": file_name,
                "file_id": file_id,
                "n_pages": n_pages,
                "content": content,
                "truncated": file_truncated,
                "source": source,    # 'compact' | 'raw_pages' (디버그·표시용)
            })

        logger.info(
            "[/glossary/inline] tenant=%s ids=%d → %d glossary files, %d chars "
            "(sources=%s, truncated=%s)",
            tenant_id, len(cleaned_ids), len(out), total_chars,
            [o["source"] for o in out], global_truncated,
        )
        return {"response": out, "total_chars": total_chars, "truncated": global_truncated}

    except Exception as e:
        logger.exception("[/glossary/inline] failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# GET /glossary/terms
# ─────────────────────────────────────────────────────────────────────────────
# 구조화 용어사전(glossary_terms) 의 tenant 전체 용어를 반환. rfi-translate 등 소비자가
# 이 목록으로 term-lock 매처를 만들어 '문서에 실제 등장한 용어만' 고정 번역한다.
# (프롬프트 통째 주입이 아니라 소비자측 스캔 → 사전이 수만 개여도 스캔은 문서 길이에만 비례)
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/glossary/terms")
async def glossary_terms(
    tenant_id: str,
    file_ids: Optional[List[str]] = Query(default=None),
):
    """구조화 용어사전 행(영문/한글뜻/약어)을 반환.

    Args:
        tenant_id: 필수.
        file_ids: 선택. 주면 해당 사전 파일들로 한정, 없으면 tenant 전체.

    Returns:
        ``{"response": [{english, korean, abbreviation}, ...], "count": int}``
    """
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    try:
        from app.services.glossary_terms import list_terms
        cleaned_ids = [str(x) for x in (file_ids or []) if x] or None
        terms = await list_terms(tenant_id, file_ids=cleaned_ids)
        logger.info(
            "[/glossary/terms] tenant=%s ids=%s → %d terms",
            tenant_id, (len(cleaned_ids) if cleaned_ids else "all"), len(terms),
        )
        return {"response": terms, "count": len(terms)}
    except Exception as e:
        logger.exception("[/glossary/terms] failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# GET /document/grep
# ─────────────────────────────────────────────────────────────────────────────

# 안전·노이즈 한도. agent 가 폭주해도 컨텍스트가 안 터지게.
_GREP_MAX_LIMIT = 100
_GREP_DEFAULT_LIMIT = 30
_GREP_SNIPPET_RADIUS = 80   # 매칭 위치 좌/우 글자 수
_GREP_MAX_CONTEXT_LINES = 5


def _split_lines_with_offset(content: str) -> List[Tuple[int, str]]:
    """본문을 (line_no_1based, line_text) 리스트로. 빈 줄도 포함."""
    return [(i + 1, line) for i, line in enumerate(content.splitlines())]


def _build_snippet(
    content: str, match_start: int, match_end: int, radius: int = _GREP_SNIPPET_RADIUS
) -> str:
    """매칭 위치 좌/우 ``radius`` 글자 스니펫 (줄바꿈은 공백으로)."""
    left = max(0, match_start - radius)
    right = min(len(content), match_end + radius)
    snippet = content[left:right].replace("\n", " ").strip()
    prefix = "…" if left > 0 else ""
    suffix = "…" if right < len(content) else ""
    return f"{prefix}{snippet}{suffix}"


@router.get("/document/grep")
async def document_grep(
    tenant_id: str,
    path: str,
    pattern: str,
    file_ids: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
    regex: bool = Query(default=False),
    case_sensitive: bool = Query(default=False),
    context_lines: int = Query(default=0, ge=0, le=_GREP_MAX_CONTEXT_LINES),
    limit: int = Query(default=_GREP_DEFAULT_LIMIT, ge=1, le=_GREP_MAX_LIMIT),
):
    """한 문서 안에서 패턴 매칭 위치 찾기.

    Args:
        tenant_id: 필수.
        path: **문서의 전체 상대경로 핸들** (open_folder/survey 출력의 path 그대로). 동명 파일도
            path 가 사업별로 유일해 정확 구별. 서버가 path → source_ref 로 해석.
        pattern: 검색 패턴. ``regex=false``(기본)면 literal substring, ``true``면 정규식.
        case_sensitive: 기본 False (대소문자 무시).
        context_lines: 매칭 라인 좌/우로 같이 돌려줄 라인 수(0~5).
        limit: 최대 매칭 수.

    Returns:
        ``{"response": [{file_name, page, line, snippet, context}, ...], "total_matches": N, "truncated": bool}``
    """
    if not tenant_id or not path or not pattern:
        raise HTTPException(status_code=400, detail="tenant_id, path, pattern required")

    file_id = await _resolve_file_id(tenant_id, path=path)
    if not file_id:
        return {
            "response": [],
            "total_matches": 0,
            "truncated": False,
            "error": f"path '{path}' not found in tenant '{tenant_id}'",
        }
    # ★ 보안 경계 — 선택한 자료(폴더 스코프 ∪ 개별 file_ids) 밖이면 본문 조회 거부.
    #   folder_paths 는 path 접두어로 순수 검사(수천 refs 열거 없이 스케일). 폴더·파일 공존 시 union.
    _allow = [str(x) for x in (file_ids or []) if x]
    _scope = [_norm_folder(x) for x in (folder_paths or []) if _norm_folder(x)]
    _path_norm = (path or "").strip().strip("/")
    if _scope or _allow:
        _in_folder = bool(_scope) and any(_path_norm == s or _path_norm.startswith(s + "/") for s in _scope)
        _in_files = bool(_allow) and (file_id in _allow)
        if not (_in_folder or _in_files):
            return {
                "response": [], "total_matches": 0, "truncated": False,
                "error": f"path '{path}' 는 선택한 자료(폴더/파일) 범위 밖입니다.",
            }
    file_name = path  # 표시/인용용 (페이지 조회는 file_id 기준)

    # 패턴 컴파일 (regex 모드면 정규식, 아니면 literal escape)
    try:
        flags = 0 if case_sensitive else re.IGNORECASE
        if regex:
            compiled = re.compile(pattern, flags)
        else:
            compiled = re.compile(re.escape(pattern), flags)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"invalid pattern: {e}")

    # 페이지 본문 조회 (정렬 보장)
    try:
        resp = await asyncio.to_thread(
            supabase.table("document_pages")
            .select("page_number, content")
            .eq("tenant_id", tenant_id)
            .eq("file_id", file_id)
            .order("page_number", desc=False)
            .execute
        )
        pages = resp.data or []
    except Exception as e:
        logger.exception("[/document/grep] page query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    matches: List[Dict[str, Any]] = []
    total_matches = 0
    truncated = False

    for page_row in pages:
        page_no = page_row.get("page_number")
        content = page_row.get("content") or ""
        if not content:
            continue

        lines_indexed = _split_lines_with_offset(content)
        for match in compiled.finditer(content):
            total_matches += 1
            if len(matches) >= limit:
                truncated = True
                continue

            # 매칭 위치의 라인 번호 계산 (페이지 내 1-based)
            line_no = content.count("\n", 0, match.start()) + 1
            line_text = ""
            if 0 < line_no <= len(lines_indexed):
                line_text = lines_indexed[line_no - 1][1]

            snippet = _build_snippet(content, match.start(), match.end())

            entry: Dict[str, Any] = {
                "file_name": file_name,
                "page": page_no,
                "line": line_no,
                "snippet": snippet,
                "match": match.group(0),
            }
            if context_lines > 0:
                start_l = max(1, line_no - context_lines)
                end_l = min(len(lines_indexed), line_no + context_lines)
                ctx = [lines_indexed[i - 1][1] for i in range(start_l, end_l + 1)]
                entry["context"] = "\n".join(ctx)
            matches.append(entry)

        # 매칭 cap 도달해도 total_matches 는 끝까지 셀 수 있도록 break 안 함.
        # 다만 총량 너무 많아지면 슬슬 빠져나가도 됨 — limit 의 2배 도달 시 cut.
        if total_matches >= limit * 5:
            truncated = True
            break

    logger.info(
        "[/document/grep] tenant=%s file=%s pattern=%r regex=%s → matches=%d total=%d",
        tenant_id, file_name, pattern[:80], regex, len(matches), total_matches,
    )
    return {
        "response": matches,
        "total_matches": total_matches,
        "truncated": truncated,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /document/page
# ─────────────────────────────────────────────────────────────────────────────

# 한 번 호출에 가져올 수 있는 최대 페이지 수 — agent 폭주 방어.
_PAGE_MAX_PER_CALL = 10


def _parse_page_range(spec: str, n_pages_hint: Optional[int] = None) -> List[int]:
    """``"5"`` / ``"5-8"`` / ``"5,7,12"`` / ``"3-5,9"`` 형식을 페이지 번호 리스트로.

    반환은 정렬·dedupe 된 1-based 페이지 번호들.
    """
    if not spec:
        return []
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                start = int(a.strip())
                end = int(b.strip())
            except ValueError:
                raise HTTPException(status_code=400, detail=f"invalid page range: {part!r}")
            if start < 1 or end < start:
                raise HTTPException(status_code=400, detail=f"invalid page range: {part!r}")
            for p in range(start, end + 1):
                out.add(p)
        else:
            try:
                p = int(part)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"invalid page number: {part!r}")
            if p < 1:
                raise HTTPException(status_code=400, detail=f"invalid page number: {part!r}")
            out.add(p)
    return sorted(out)


@router.get("/document/page")
async def document_page(
    tenant_id: str,
    path: str,
    pages: str,
    file_ids: Optional[List[str]] = Query(default=None),
    folder_paths: Optional[List[str]] = Query(default=None),
):
    """페이지 범위 본문 반환.

    Args:
        tenant_id: 필수.
        path: **문서의 전체 상대경로 핸들** (open_folder/survey 출력의 path 그대로). 서버가
            path → source_ref 로 해석. 동명 파일도 path 가 사업별로 유일해 정확 구별.
        pages: ``"5"`` / ``"5-8"`` / ``"5,7,12"`` / ``"3-5,9"`` 형식. 한 번 호출 최대 10페이지.

    Returns:
        ``{"file_name", "pages": [{"page_number", "content"}, ...]}``
    """
    if not tenant_id or not path or not pages:
        raise HTTPException(status_code=400, detail="tenant_id, path, pages required")

    page_numbers = _parse_page_range(pages)
    if not page_numbers:
        raise HTTPException(status_code=400, detail="no pages parsed from 'pages'")
    if len(page_numbers) > _PAGE_MAX_PER_CALL:
        raise HTTPException(
            status_code=400,
            detail=(
                f"too many pages requested ({len(page_numbers)}); "
                f"max {_PAGE_MAX_PER_CALL} per call"
            ),
        )

    file_id = await _resolve_file_id(tenant_id, path=path)
    file_name = path  # 응답 표시용 (페이지 조회는 file_id 기준)
    if not file_id:
        return {
            "file_name": file_name,
            "pages": [],
            "error": f"path '{path}' not found in tenant '{tenant_id}'",
        }
    # ★ 보안 경계 — 선택한 자료(폴더 스코프 ∪ 개별 file_ids) 밖이면 본문 조회 거부. 공존 시 union.
    _allow = [str(x) for x in (file_ids or []) if x]
    _scope = [_norm_folder(x) for x in (folder_paths or []) if _norm_folder(x)]
    _path_norm = (path or "").strip().strip("/")
    if _scope or _allow:
        _in_folder = bool(_scope) and any(_path_norm == s or _path_norm.startswith(s + "/") for s in _scope)
        _in_files = bool(_allow) and (file_id in _allow)
        if not (_in_folder or _in_files):
            return {
                "file_name": file_name, "pages": [],
                "error": f"path '{path}' 는 선택한 자료(폴더/파일) 범위 밖입니다.",
            }

    # 다운로드 핸들 — 출처 칩에서 원본 파일을 내려받게 source_type/source_ref/실제 file_name 동봉.
    # (source_ref = drive: google file_id / upload: storage_path)
    storage_type = ""
    real_file_name = ""
    try:
        meta_resp = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_type, file_name")
            .eq("tenant_id", tenant_id).eq("source_ref", file_id)
            .limit(1).execute
        )
        meta_row = (meta_resp.data or [{}])[0] if meta_resp.data else {}
        storage_type = str(meta_row.get("source_type") or "")
        real_file_name = str(meta_row.get("file_name") or "")
    except Exception as e:  # noqa: BLE001 — 다운로드 핸들 조회 실패가 페이지 반환을 막지 않게
        logger.warning("[/document/page] source meta lookup failed: %s", e)

    try:
        resp = await asyncio.to_thread(
            supabase.table("document_pages")
            .select("page_number, content")
            .eq("tenant_id", tenant_id)
            .eq("file_id", file_id)
            .in_("page_number", page_numbers)
            .order("page_number", desc=False)
            .execute
        )
        rows = resp.data or []
    except Exception as e:
        logger.exception("[/document/page] query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    out_pages = [
        {"page_number": r.get("page_number"), "content": r.get("content") or ""}
        for r in rows
    ]
    logger.info(
        "[/document/page] tenant=%s file=%s req=%s → pages=%d",
        tenant_id, file_name, pages, len(out_pages),
    )
    return {
        "file_name": real_file_name or file_name,
        "file_id": file_id,
        "source_ref": file_id,
        "storage_type": storage_type,
        "pages": out_pages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /document/raw
#
# 원본 파일 바이트 스트림 — Codex가 선택한 문서의 작업 복사본을 만들 때 호출한다.
# drive 소스가 아닌 upload 소스만 허용.
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/document/raw")
async def document_raw(
    tenant_id: str,
    file_id: Optional[str] = None,
    file_name: Optional[str] = None,
):
    """파일 원본 바이트를 binary stream 으로 반환.

    Args:
        tenant_id와 file_id가 정본이다. file_name은 구 클라이언트 호환용 폴백이다.

    제약:
        - upload 소스만 허용 (storage 'files' 버킷에서 다운로드).
        - drive 소스는 미지원 — drive 원본은 별도 OAuth 흐름이 필요.

    Returns:
        ``application/octet-stream`` body. ``Content-Disposition`` 에 file_name 포함.
    """
    from fastapi import Response

    if not tenant_id or not (file_id or file_name):
        raise HTTPException(status_code=400, detail="tenant_id and file_id (or legacy file_name) required")

    try:
        query = (
            supabase.table("knowledge_files")
            .select("source_ref, source_type, file_name, mime_type, size_bytes, file_hash")
            .eq("tenant_id", tenant_id)
        )
        if file_id:
            query = query.eq("source_ref", file_id)
        else:
            query = query.eq("file_name", file_name).order("modified_time", desc=True)
        result = await asyncio.to_thread(query.limit(1).execute)
        rows = result.data or []
    except Exception as e:
        logger.exception("[/document/raw] resolve failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    if not rows:
        locator = f"file_id '{file_id}'" if file_id else f"file_name '{file_name}'"
        raise HTTPException(status_code=404, detail=f"{locator} not found in tenant '{tenant_id}'")
    row = rows[0]
    resolved_name = str(row.get("file_name") or file_name or "file")
    source_type = row.get("source_type")
    source_ref = row.get("source_ref")
    if source_type != "upload":
        raise HTTPException(
            status_code=400,
            detail=f"only upload-source files supported (got source_type='{source_type}')",
        )
    if not source_ref:
        raise HTTPException(status_code=500, detail="source_ref empty")

    try:
        data: bytes = await asyncio.to_thread(
            supabase.storage.from_("files").download, source_ref
        )
    except Exception as e:
        logger.exception("[/document/raw] download failed (path=%s): %s", source_ref, e)
        raise HTTPException(status_code=500, detail=f"storage download failed: {e}")

    if not data:
        raise HTTPException(status_code=404, detail="empty file")

    logger.info(
        "[/document/raw] tenant=%s file=%s bytes=%d",
        tenant_id, resolved_name, len(data),
    )
    # Content-Disposition 의 filename 은 ASCII-safe 한 fallback + RFC 5987 utf-8 양쪽 제공.
    # ⚠ ``isalnum()`` 은 한글도 True 라서 그대로 쓰면 latin-1 헤더 인코딩 실패.
    # ASCII 영역의 alnum 만 통과시키고 나머지는 ``_`` 로 치환.
    import urllib.parse
    safe_name = "".join(
        c if (c.isascii() and (c.isalnum() or c in "._-")) else "_"
        for c in resolved_name
    ).strip("_") or "file"
    quoted = urllib.parse.quote(resolved_name)
    response_headers = {
        "Content-Disposition": f"attachment; filename=\"{safe_name}\"; filename*=UTF-8''{quoted}",
        "X-ProcessGPT-File-Id": str(source_ref),
    }
    if row.get("file_hash"):
        response_headers["X-ProcessGPT-Sha256"] = str(row["file_hash"])
    return Response(
        content=data,
        media_type=str(row.get("mime_type") or "application/octet-stream"),
        headers=response_headers,
    )
