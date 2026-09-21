"""에이전트 산출물은 비공개 버킷에 둔다.

`files` 버킷은 공개다. 채팅 첨부·본문 이미지·폴더 원본이 모두 그 공개 주소를 그대로 쓰고
있어서 지금 와서 닫을 수 없다. 그런데 에이전트가 만드는 것은 계약서 검토 결과나 사내
보고서다 — 주소만 알면 누구나, 언제까지나 열리는 자리에 둘 것이 아니다.

그래서 산출물만 **따로 비공개 버킷**에 넣고, 볼 때마다 만료되는 서명 주소를 발급한다.
공개 버킷을 통째로 닫는 것보다 옮길 것이 적고, 되돌릴 때도 이 파일 하나만 보면 된다.

객체 키는 `artifacts/` 로 시작한다. 어느 버킷에서 꺼내야 하는지를 키만 보고 알기 위해서다
(`bucket_for`). 키를 저장하는 곳(knowledge_files.source_ref)이 이미 여럿이라, 버킷을 따로
들고 다니게 하면 그 자리마다 컬럼을 늘려야 한다.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.supabase_client import supabase

# 공개 버킷. 첨부·이미지·폴더 원본이 쓴다.
PUBLIC_BUCKET = "files"
# 산출물 전용 비공개 버킷. 공개 정책 없이 만들어 둬야 한다(docs/artifact-bucket.md).
ARTIFACT_BUCKET = os.getenv("ARTIFACT_BUCKET", "artifacts")
# 이 접두사로 시작하는 키는 비공개 버킷에 있다.
ARTIFACT_PREFIX = "artifacts/"

# 서명 주소 기본 수명. 한 번의 대화에서 보고 받기에 넉넉하고, 유출돼도 오래 살지 않을 만큼 짧다.
DEFAULT_TTL_SECONDS = int(os.getenv("ARTIFACT_URL_TTL_SECONDS", "3600"))


def is_artifact_key(key: str) -> bool:
    return str(key or "").startswith(ARTIFACT_PREFIX)


def bucket_for(key: str) -> str:
    """이 객체 키가 들어 있는 버킷."""
    return ARTIFACT_BUCKET if is_artifact_key(key) else PUBLIC_BUCKET


def new_key(file_name: str) -> str:
    """산출물의 객체 키. 이름은 추측할 수 없게 하고 확장자만 남긴다."""
    return f"{ARTIFACT_PREFIX}{uuid.uuid4()}{Path(file_name or '').suffix.lower()}"


def signed_url(key: str, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> tuple[str, str]:
    """비공개 산출물의 서명 주소와 만료 시각(ISO8601)을 돌려준다.

    공개 버킷의 키가 들어오면 공개 주소를 돌려준다 — 옛 산출물은 아직 거기 있다.
    """
    if not is_artifact_key(key):
        response = supabase.storage.from_(PUBLIC_BUCKET).get_public_url(key)
        url = response.get("publicURL", "") if isinstance(response, dict) else str(response)
        return url, ""

    response = supabase.storage.from_(ARTIFACT_BUCKET).create_signed_url(key, ttl_seconds)
    url = ""
    if isinstance(response, dict):
        url = response.get("signedURL") or response.get("signedUrl") or response.get("signed_url") or ""
    if not url:
        raise RuntimeError(f"failed to sign artifact url: {key}")
    if url.startswith("/"):
        url = f"{os.getenv('SUPABASE_URL', '').rstrip('/')}/storage/v1{url}"
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
    return url, expires_at


__all__ = [
    "ARTIFACT_BUCKET",
    "ARTIFACT_PREFIX",
    "DEFAULT_TTL_SECONDS",
    "PUBLIC_BUCKET",
    "bucket_for",
    "is_artifact_key",
    "new_key",
    "signed_url",
]
