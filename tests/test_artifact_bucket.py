# -*- coding: utf-8 -*-
"""산출물이 공개 버킷으로 새지 않는가, 그리고 옛 산출물이 그대로 열리는가.

에이전트가 만드는 것은 계약서 검토 결과나 사내 보고서다. 주소만 알면 누구나, 언제까지나
열리는 자리에 둘 것이 아니다(docs/artifact-bucket.md). 그런데 `files` 버킷은 공개이고,
채팅 첨부가 이미 거기 있다 — 두 갈래를 키 하나로 가려낸다.
"""
import sys
import types

import pytest


@pytest.fixture
def bucket(monkeypatch):
    """supabase 클라이언트를 세우지 않고 모듈만 불러온다."""
    fake = types.ModuleType("app.core.supabase_client")
    fake.supabase = object()
    monkeypatch.setitem(sys.modules, "app.core.supabase_client", fake)
    sys.modules.pop("app.storage.artifact_bucket", None)
    import app.storage.artifact_bucket as module

    yield module
    sys.modules.pop("app.storage.artifact_bucket", None)


def test_산출물_키는_비공개_버킷을_가리킨다(bucket):
    key = bucket.new_key("2026년 보고서.docx")

    assert key.startswith(bucket.ARTIFACT_PREFIX)
    assert bucket.bucket_for(key) == bucket.ARTIFACT_BUCKET


def test_키에_원래_이름을_남기지_않는다(bucket):
    """이름이 곧 내용을 말한다. '계약서_검토_최종.docx' 를 주소에 실을 이유가 없다."""
    key = bucket.new_key("계약서_검토_최종.docx")

    assert "계약서" not in key
    assert key.endswith(".docx"), "확장자는 남겨야 브라우저가 무엇인지 안다"


def test_옛_산출물은_아직_공개_버킷에_있다(bucket):
    """산출물이 있는 기존 대화도 그대로 열려야 한다 — 옮기지 않고 키로 가려낸다."""
    assert bucket.bucket_for("files/20260101_report.pdf") == bucket.PUBLIC_BUCKET
    assert bucket.is_artifact_key("files/20260101_report.pdf") is False


def test_서명_주소의_수명은_한_시간(bucket):
    """한 번의 대화에서 보고 받기에 넉넉하고, 새어 나가도 오래 살지 않을 만큼 짧다."""
    assert bucket.DEFAULT_TTL_SECONDS == 3600
