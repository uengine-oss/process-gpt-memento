"""구조화 용어사전(영문/한글뜻/약어) 파싱·저장·조회.

고객 제공 CSV 는 헤더가 **항상 고정**: ``영문,한글뜻,약어``.
값에는 빈 칸(특히 약어)이 있을 수 있고, 영문 필드 안에 콤마가 들어올 수도 있다.

흐름:
    upload(doc_role='glossary') → knowledge_admin 이 이 모듈의 parse → replace_file_terms 호출.
    (구조화 파싱 성공 시 기존 LLM glossary_compact 추출은 건너뜀)

저장 위치: public.glossary_terms (sql/glossary_terms.sql). file_id(=storage_path) 단위 replace.
소비자: rfi-translate 가 GET /glossary/terms 로 tenant 전체 용어를 받아 term-lock 매처를 만든다.
"""
from __future__ import annotations

import csv
import io
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.supabase_client import supabase

logger = logging.getLogger(__name__)

# 고정 헤더 — 별칭 최소 허용(공백/영문 표기 흔들림 흡수). 핵심은 '영문' + '한글뜻'.
_H_ENGLISH = {"영문", "영어", "english", "영문명", "term", "영문용어"}
_H_KOREAN = {"한글뜻", "한글", "뜻", "국문", "korean", "meaning", "한글의미", "한글명"}
_H_ABBR = {"약어", "약자", "abbr", "abbreviation", "acronym"}

_STRUCTURED_EXTS = {".csv", ".tsv", ".txt", ".xlsx", ".xls"}

# 재업로드 시 대량 삭제 방지용 안전 상한(비정상 파일 방어).
_MAX_ROWS = 200_000


# ─────────────────────────────────────────────────────────────────────────────
# 정규화
# ─────────────────────────────────────────────────────────────────────────────

def normalize_english(text: str) -> str:
    """영문 표제어/구문 매칭 키 — 소문자화 + 비영숫자를 공백으로 접고 공백 1칸으로 정규화.

    term-lock 매처가 문서 텍스트에도 같은 정규화를 적용해 대조하므로,
    데이터에 섞인 앞뒤/중간 공백·구두점 흔들림을 흡수한다.
    """
    if not text:
        return ""
    low = re.sub(r"[^0-9a-z가-힣]+", " ", str(text).lower())
    return re.sub(r"\s+", " ", low).strip()


def normalize_abbr(text: str) -> str:
    """약어 매칭 키 — 대문자화 + 영숫자만 남김 (RLI, CIGRE 등 정확 토큰 매칭용)."""
    if not text:
        return ""
    return re.sub(r"[^0-9A-Z]+", "", str(text).upper())


def _header_key(cell: str) -> str:
    return re.sub(r"\s+", "", str(cell or "")).lower().lstrip("﻿")


# ─────────────────────────────────────────────────────────────────────────────
# 파싱
# ─────────────────────────────────────────────────────────────────────────────

def _decode_bytes(data: bytes) -> str:
    """CSV/TXT 바이트 → 문자열. 한국어 파일 흔한 인코딩 순차 시도."""
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _rows_from_csv(data: bytes) -> List[List[str]]:
    text = _decode_bytes(data)
    # 구분자 자동 추정(콤마/탭/세미콜론) — 실패하면 콤마.
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = "\t" if sample.count("\t") > sample.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    return [[(c or "").strip() for c in row] for row in reader]


def _rows_from_xlsx(data: bytes) -> List[List[str]]:
    try:
        from openpyxl import load_workbook
    except Exception as e:  # openpyxl 미설치 — CSV 로 유도
        raise RuntimeError(f"xlsx 파싱 불가(openpyxl 없음): {e}")
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active
    rows: List[List[str]] = []
    for row in ws.iter_rows(values_only=True):
        rows.append([("" if c is None else str(c)).strip() for c in row])
    wb.close()
    return rows


def _detect_columns(header: List[str]) -> Optional[Dict[str, int]]:
    """헤더 행 → {english, korean, abbreviation} 컬럼 인덱스. 필수(영문·한글뜻) 없으면 None."""
    idx = {"english": -1, "korean": -1, "abbreviation": -1}
    for i, cell in enumerate(header):
        key = _header_key(cell)
        if idx["english"] < 0 and key in _H_ENGLISH:
            idx["english"] = i
        elif idx["korean"] < 0 and key in _H_KOREAN:
            idx["korean"] = i
        elif idx["abbreviation"] < 0 and key in _H_ABBR:
            idx["abbreviation"] = i
    if idx["english"] < 0 or idx["korean"] < 0:
        return None
    return idx


def parse_glossary_terms(file_content: bytes, file_name: str) -> Optional[List[Dict[str, str]]]:
    """고정형(영문,한글뜻,약어) 용어사전 파일을 행 리스트로 파싱.

    Returns:
        [{"english","korean","abbreviation"}, ...]  — 구조화 사전으로 인식된 경우.
        None — 확장자/헤더가 고정형이 아니어서 구조화 파싱 대상이 아님(→ 기존 compact 경로).

    콤마가 영문 필드에 섞인 비정상 행 방어: 컬럼 매핑은 헤더 인덱스로 하되,
    행 셀 수가 헤더보다 많으면 뒤쪽(약어·한글뜻)을 우선 고정하고 나머지를 영문으로 합친다.
    """
    ext = Path(file_name or "").suffix.lower()
    if ext not in _STRUCTURED_EXTS:
        return None

    try:
        rows = _rows_from_xlsx(file_content) if ext in (".xlsx", ".xls") else _rows_from_csv(file_content)
    except Exception as e:
        logger.warning("[glossary_terms] parse rows failed (%s): %s", file_name, e)
        return None

    # 첫 비어있지 않은 행 = 헤더.
    header_idx = next((i for i, r in enumerate(rows) if any((c or "").strip() for c in r)), -1)
    if header_idx < 0:
        return None
    header = rows[header_idx]
    cols = _detect_columns(header)
    if cols is None:
        return None  # 고정 헤더 아님 → 구조화 사전 아님

    n_cols = len(header)
    ci_en, ci_ko, ci_ab = cols["english"], cols["korean"], cols["abbreviation"]

    out: List[Dict[str, str]] = []
    for r in rows[header_idx + 1:]:
        if not any((c or "").strip() for c in r):
            continue
        cells = list(r)
        # 셀 수가 헤더보다 많으면(영문 안 콤마 등) 뒤에서부터 정렬 — 약어/한글은 콤마가 거의 없다.
        if len(cells) > n_cols and ci_en == 0:
            # 표준 배치(영문,한글뜻,약어)에서 영문이 첫 칼럼일 때만 병합 휴리스틱 적용.
            tail = cells[-(n_cols - 1):] if n_cols > 1 else []
            head = ",".join(cells[: len(cells) - len(tail)]).strip()
            merged = [head] + tail
            english = merged[ci_en] if ci_en < len(merged) else ""
            korean = merged[ci_ko] if ci_ko < len(merged) else ""
            abbreviation = merged[ci_ab] if 0 <= ci_ab < len(merged) else ""
        else:
            english = cells[ci_en] if ci_en < len(cells) else ""
            korean = cells[ci_ko] if ci_ko < len(cells) else ""
            abbreviation = cells[ci_ab] if 0 <= ci_ab < len(cells) else ""

        english = (english or "").strip()
        korean = (korean or "").strip()
        abbreviation = (abbreviation or "").strip()
        # 검색 키(영문/약어)가 둘 다 없으면 쓸모 없음 → skip.
        if not english and not abbreviation:
            continue
        out.append({"english": english, "korean": korean, "abbreviation": abbreviation})
        if len(out) >= _MAX_ROWS:
            logger.warning("[glossary_terms] %s: row cap %d 도달 — 이후 무시", file_name, _MAX_ROWS)
            break

    return out


def looks_like_structured_glossary(file_content: bytes, file_name: str) -> bool:
    """구조화(고정형) 용어사전 파일인지 여부 — 인제스트 분기용 가벼운 판별."""
    ext = Path(file_name or "").suffix.lower()
    if ext not in _STRUCTURED_EXTS:
        return False
    parsed = parse_glossary_terms(file_content, file_name)
    return parsed is not None


# ─────────────────────────────────────────────────────────────────────────────
# 저장 / 조회 (Supabase)
# ─────────────────────────────────────────────────────────────────────────────

async def replace_file_terms(
    tenant_id: str,
    file_id: str,
    file_name: str,
    rows: List[Dict[str, str]],
) -> int:
    """file_id 의 기존 용어를 모두 지우고 rows 로 다시 채움 (재업로드 멱등).

    Returns: 저장된 용어 수.
    """
    import asyncio

    if not tenant_id or not file_id:
        return 0

    # 기존 삭제 — 재업로드/재인덱싱 멱등.
    try:
        await asyncio.to_thread(
            supabase.table("glossary_terms")
            .delete()
            .eq("tenant_id", tenant_id)
            .eq("file_id", file_id)
            .execute
        )
    except Exception as e:
        logger.warning("[glossary_terms] delete old rows failed (%s/%s): %s", tenant_id, file_id, e)

    if not rows:
        return 0

    payload = [
        {
            "tenant_id": tenant_id,
            "file_id": file_id,
            "file_name": file_name or "",
            "english": r.get("english", ""),
            "korean": r.get("korean", ""),
            "abbreviation": r.get("abbreviation", ""),
            "english_norm": normalize_english(r.get("english", "")),
            "abbr_norm": normalize_abbr(r.get("abbreviation", "")),
        }
        for r in rows
    ]

    # 대량 insert 는 청크로 나눠(요청 크기 한도 회피).
    saved = 0
    chunk = 1000
    for i in range(0, len(payload), chunk):
        part = payload[i:i + chunk]
        try:
            await asyncio.to_thread(
                supabase.table("glossary_terms").insert(part).execute
            )
            saved += len(part)
        except Exception as e:
            logger.warning("[glossary_terms] insert chunk failed (%s rows): %s", len(part), e)
    logger.info("[glossary_terms] %s: replaced with %d terms (file=%s)", tenant_id, saved, file_name)
    return saved


async def list_terms(
    tenant_id: str,
    file_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """tenant(옵션: 특정 file_ids)의 용어 전체를 반환 — term-lock 매처 빌드용.

    Returns: [{english, korean, abbreviation}, ...] (정규화 컬럼은 소비자가 재계산하므로 제외).
    """
    import asyncio

    if not tenant_id:
        return []

    def _query():
        q = (
            supabase.table("glossary_terms")
            .select("english, korean, abbreviation")
            .eq("tenant_id", tenant_id)
        )
        if file_ids:
            q = q.in_("file_id", [str(x) for x in file_ids if x])
        return q.execute()

    try:
        resp = await asyncio.to_thread(_query)
        return resp.data or []
    except Exception as e:
        logger.warning("[glossary_terms] list_terms failed (%s): %s", tenant_id, e)
        return []
