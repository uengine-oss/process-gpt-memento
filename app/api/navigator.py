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

router = APIRouter()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 헬퍼 — file_name → file_id (source_ref) 해석
# ─────────────────────────────────────────────────────────────────────────────

async def _resolve_file_id(tenant_id: str, file_name: str) -> Optional[str]:
    """동일 tenant 안에서 file_name 매칭되는 첫 knowledge_files row 의 source_ref 반환.

    동일 이름 파일이 여러 개면 가장 최근 modified 한 거 선택.
    """
    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref, source_type, modified_time, indexed_at")
            .eq("tenant_id", tenant_id)
            .eq("file_name", file_name)
            .order("modified_time", desc=True)
            .limit(1)
            .execute
        )
        rows = result.data or []
        if not rows:
            return None
        return rows[0].get("source_ref")
    except Exception as e:
        logger.warning(
            "[navigator] resolve file_id failed (tenant=%s, name=%s): %s",
            tenant_id, file_name, e,
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

    try:
        query = (
            supabase.table("knowledge_files")
            .select(
                "source_ref, source_type, file_name, folder_path, mime_type, "
                "size_bytes, modified_time, indexed_at, index_status, doc_card, doc_role"
            )
            .eq("tenant_id", tenant_id)
        )

        cleaned_ids = [str(x) for x in (file_ids or []) if x]
        cleaned_names = [str(x) for x in (file_names or []) if x]
        if cleaned_ids:
            query = query.in_("source_ref", cleaned_ids)
        elif cleaned_names:
            query = query.in_("file_name", cleaned_names)

        query = query.order("file_name", desc=False)

        response = await asyncio.to_thread(query.execute)
        rows = response.data or []

        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "file_id": r.get("source_ref"),
                "file_name": r.get("file_name"),
                "folder_path": r.get("folder_path") or "",
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
    file_name: str,
    pattern: str,
    regex: bool = Query(default=False),
    case_sensitive: bool = Query(default=False),
    context_lines: int = Query(default=0, ge=0, le=_GREP_MAX_CONTEXT_LINES),
    limit: int = Query(default=_GREP_DEFAULT_LIMIT, ge=1, le=_GREP_MAX_LIMIT),
):
    """한 문서 안에서 패턴 매칭 위치 찾기.

    Args:
        tenant_id, file_name: 필수.
        pattern: 검색 패턴. ``regex=false``(기본)면 literal substring, ``true``면 정규식.
        case_sensitive: 기본 False (대소문자 무시).
        context_lines: 매칭 라인 좌/우로 같이 돌려줄 라인 수(0~5).
        limit: 최대 매칭 수.

    Returns:
        ``{"response": [{file_name, page, line, snippet, context}, ...], "total_matches": N, "truncated": bool}``
    """
    if not tenant_id or not file_name or not pattern:
        raise HTTPException(status_code=400, detail="tenant_id, file_name, pattern required")

    file_id = await _resolve_file_id(tenant_id, file_name)
    if not file_id:
        return {
            "response": [],
            "total_matches": 0,
            "truncated": False,
            "error": f"file_name '{file_name}' not found in tenant '{tenant_id}'",
        }

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
    file_name: str,
    pages: str,
):
    """페이지 범위 본문 반환.

    Args:
        tenant_id, file_name: 필수.
        pages: ``"5"`` / ``"5-8"`` / ``"5,7,12"`` / ``"3-5,9"`` 형식. 한 번 호출 최대 10페이지.

    Returns:
        ``{"file_name", "pages": [{"page_number", "content"}, ...]}``
    """
    if not tenant_id or not file_name or not pages:
        raise HTTPException(status_code=400, detail="tenant_id, file_name, pages required")

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

    file_id = await _resolve_file_id(tenant_id, file_name)
    if not file_id:
        return {
            "file_name": file_name,
            "pages": [],
            "error": f"file_name '{file_name}' not found in tenant '{tenant_id}'",
        }

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
    return {"file_name": file_name, "pages": out_pages}


# ─────────────────────────────────────────────────────────────────────────────
# GET /document/raw
#
# 원본 파일 바이트 스트림 — data-analyst 서브에이전트가 sandbox 안에서 코드로 처리할
# dataset (xlsx/csv) 파일을 받기 위해 호출. drive 소스가 아닌 upload 소스만 허용.
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/document/raw")
async def document_raw(
    tenant_id: str,
    file_name: str,
):
    """파일 원본 바이트를 binary stream 으로 반환.

    Args:
        tenant_id, file_name: 필수.

    제약:
        - upload 소스만 허용 (storage 'files' 버킷에서 다운로드).
        - drive 소스는 미지원 — drive 원본은 별도 OAuth 흐름이 필요.

    Returns:
        ``application/octet-stream`` body. ``Content-Disposition`` 에 file_name 포함.
    """
    from fastapi import Response

    if not tenant_id or not file_name:
        raise HTTPException(status_code=400, detail="tenant_id, file_name required")

    try:
        result = await asyncio.to_thread(
            supabase.table("knowledge_files")
            .select("source_ref, source_type")
            .eq("tenant_id", tenant_id)
            .eq("file_name", file_name)
            .order("modified_time", desc=True)
            .limit(1)
            .execute
        )
        rows = result.data or []
    except Exception as e:
        logger.exception("[/document/raw] resolve failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    if not rows:
        raise HTTPException(status_code=404, detail=f"file_name '{file_name}' not found in tenant '{tenant_id}'")
    row = rows[0]
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
        tenant_id, file_name, len(data),
    )
    # Content-Disposition 의 filename 은 ASCII-safe 한 fallback + RFC 5987 utf-8 양쪽 제공.
    # ⚠ ``isalnum()`` 은 한글도 True 라서 그대로 쓰면 latin-1 헤더 인코딩 실패.
    # ASCII 영역의 alnum 만 통과시키고 나머지는 ``_`` 로 치환.
    import urllib.parse
    safe_name = "".join(
        c if (c.isascii() and (c.isalnum() or c in "._-")) else "_"
        for c in file_name
    ).strip("_") or "file"
    quoted = urllib.parse.quote(file_name)
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename=\"{safe_name}\"; filename*=UTF-8''{quoted}",
        },
    )
