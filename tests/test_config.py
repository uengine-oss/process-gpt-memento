# -*- coding: utf-8 -*-
"""프로바이더 설정: 새 이름 한 벌, 예전 이름은 경고하며 읽는다. 목록은 docs/specs/configuration.md."""
import pytest

from app.core import config

OLD = ["OPENAI_LLM_API_KEY", "LLM_API_KEY", "LLM_PROXY_API_KEY", "OPENAI_API_KEY", "OPENAI_LLM_BASE_URL", "LLM_BASE_URL",
       "LLM_PROXY_URL", "OPENAI_LLM_MODEL", "LLM_MODEL", "CUSTOM_LLM_BASE_URL", "CUSTOM_LLM_API_KEY", "CUSTOM_LLM_MODEL",
       "OPENAI_EMBEDDING_API_KEY", "EMBEDDING_API_KEY", "OPENAI_EMBEDDING_BASE_URL", "EMBEDDING_BASE_URL",
       "OPENAI_EMBEDDING_MODEL", "LLM_EMBEDDING_MODEL", "EMBEDDING_BATCH_SIZE"]
NEW = ["MEMENTO_LLM_PROVIDER", "MEMENTO_LLM_BASE_URL", "MEMENTO_LLM_API_KEY", "MEMENTO_LLM_MODEL",
       "MEMENTO_EMBEDDING_PROVIDER", "MEMENTO_EMBEDDING_BASE_URL", "MEMENTO_EMBEDDING_API_KEY", "MEMENTO_EMBEDDING_MODEL",
       "MEMENTO_EMBEDDING_BATCH_SIZE", "MEMENTO_TABLE_LLM_MODEL", "QDRANT_COLLECTION_NAME", "KB_CARD_CONCURRENCY"]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for n in OLD + NEW + list(config.IGNORED_ENV) + ["CHROMA_COLLECTION_NAME"]:
        monkeypatch.delenv(n, raising=False)
    config._warned_legacy.clear()


def test_new_names_configure_the_llm(monkeypatch):
    monkeypatch.setenv("MEMENTO_LLM_PROVIDER", "custom")
    monkeypatch.setenv("MEMENTO_LLM_BASE_URL", "http://gpu:30000/v1")
    monkeypatch.setenv("MEMENTO_LLM_API_KEY", "k")
    monkeypatch.setenv("MEMENTO_LLM_MODEL", "frentis-ai-model")
    cfg = config.resolve_llm_config()
    assert (cfg["provider"], cfg["base_url"], cfg["api_key"], cfg["model"]) == ("custom", "http://gpu:30000/v1", "k", "frentis-ai-model")


def test_provider_preset_fills_defaults():
    cfg = config.resolve_llm_config()
    assert (cfg["provider"], cfg["base_url"], cfg["model"]) == ("openai", "https://api.openai.com/v1", "gpt-5.6-luna")


def test_old_names_still_work_and_warn(monkeypatch, capsys):
    # 운영 k8s 가 지금 넣는 이름
    monkeypatch.setenv("LLM_BASE_URL", "http://litellm-proxy:4000/v1")
    monkeypatch.setenv("LLM_MODEL", "frentis-ai-model")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    monkeypatch.setenv("EMBEDDING_BASE_URL", "http://litellm-proxy:4000/v1")
    monkeypatch.setenv("OPENAI_EMBEDDING_MODEL", "qwen/qwen3-embedding-4b")
    llm, emb = config.resolve_llm_config(), config.resolve_embedding_config()
    assert (llm["base_url"], llm["model"], llm["api_key"]) == ("http://litellm-proxy:4000/v1", "frentis-ai-model", "k")
    assert (emb["base_url"], emb["model"], emb["api_key"]) == ("http://litellm-proxy:4000/v1", "qwen/qwen3-embedding-4b", "k")
    assert "LLM_BASE_URL 는 예전 이름이다 — MEMENTO_LLM_BASE_URL" in capsys.readouterr().out


def test_new_name_wins_over_old(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "old")
    monkeypatch.setenv("MEMENTO_LLM_MODEL", "new")
    assert config.resolve_llm_config()["model"] == "new"


def test_old_names_of_another_provider_are_ignored(monkeypatch):
    monkeypatch.setenv("MEMENTO_LLM_PROVIDER", "custom")
    monkeypatch.setenv("MEMENTO_LLM_BASE_URL", "http://gpu/v1")
    monkeypatch.setenv("LLM_MODEL", "openai-only")  # openai 의 예전 이름
    assert config.resolve_llm_config()["model"] == "/models/openai/gpt-oss-120b"


def test_custom_needs_a_base_url(monkeypatch):
    monkeypatch.setenv("MEMENTO_LLM_PROVIDER", "custom")
    with pytest.raises(ValueError, match="MEMENTO_LLM_BASE_URL"):
        config.resolve_llm_config()


def test_embedding_batch_size(monkeypatch):
    assert config.embedding_batch_size() == 8
    monkeypatch.setenv("EMBEDDING_BATCH_SIZE", "64")
    assert config.embedding_batch_size() == 64
    monkeypatch.setenv("MEMENTO_EMBEDDING_BATCH_SIZE", "32")
    assert config.embedding_batch_size() == 32


def test_collection_name_keeps_an_old_override(monkeypatch, capsys):
    assert config.qdrant_collection_name() == "documents"
    monkeypatch.setenv("QDRANT_COLLECTION_NAME", "dev_docs")
    assert config.qdrant_collection_name() == "dev_docs"
    assert "QDRANT_COLLECTION_NAME=dev_docs 를 아직 따르지만" in capsys.readouterr().out


def test_ignored_names_are_reported(monkeypatch, capsys):
    monkeypatch.setenv("KB_CARD_CONCURRENCY", "3")
    assert config.warn_ignored_env() == ["KB_CARD_CONCURRENCY"]
    assert "KB_CARD_CONCURRENCY 는 더 이상 읽지 않는다" in capsys.readouterr().out
