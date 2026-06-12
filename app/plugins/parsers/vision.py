"""PDF Vision(멀티모달 LLM) 헬퍼 — 내부 PoC용.

두 가지 호출:
  1. ``ocr_page_image`` — 텍스트 레이어 없는 스캔/이미지 페이지를 통째로 OCR
     (본문은 받아쓰고 도식은 ``[도식: ...]`` 으로 설명).
  2. ``describe_image`` — 텍스트 레이어가 있는 페이지에 삽입된 개별 그림/도식을 설명.
     (텍스트는 파서가 뽑고, 그림만 VLM 으로 돌려 *그림 자리에* inline 삽입하는 용도.)

provider 설정은 memento 의 ``resolve_llm_config()`` 사용(openai/openrouter/custom 공통).
활성화는 전용 토글 ``MEMENTO_PDF_VISION`` (기본 on) — 이미지 추출-업로드 경로와 무관.
"""
from __future__ import annotations

import base64
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Tuple

import httpx

from app.core.config import resolve_llm_config


# ─── 동작 상수 (env 아님 — parsers/config.py 의 Synap 상수와 동일 컨벤션) ──────
PDF_VISION_ENABLED = True       # PDF 스캔/이미지 페이지 VLM 처리 on/off
# 문서 전체 VLM 동시 호출 수 (ThreadPool). frentis vision 서버 실측: 4=안정(10p 19s),
# 10=전부 500(단일 GPU 동시추론 한계). 서버 증설 전까지 4 유지.
VISION_MAX_WORKERS = 4
OCR_MAX_TOKENS = 8192           # 스캔 페이지 통합 OCR
DESCRIBE_MAX_TOKENS = 4096      # 개별 그림 설명
VISION_TIMEOUT_SEC = 300.0      # 개별 호출 timeout(초)


# 스캔 페이지 전체 OCR — 본문은 받아쓰고 도식만 설명 모드로.
UNIFIED_OCR_PROMPT = (
    "당신은 정밀 문서 OCR 엔진입니다. 주어진 문서 페이지 이미지를 보고 "
    "**페이지에 보이는 모든 텍스트를 원문 그대로** 추출하세요. 규칙:\n"
    "1. 한글/영문/숫자/기호를 빠짐없이, 읽는 순서(위→아래, 좌→우)대로 출력.\n"
    "2. 표는 마크다운 표로 구조를 유지해서 옮길 것 (열/행 순서 보존).\n"
    "3. 제목/소제목/글머리표(•, -, □, 1.)는 그대로 유지.\n"
    "4. 도식/플로차트/조직도/차트를 만나면 그대로 받아쓰지 말고 "
    "`[도식: 구성요소·관계·화살표 방향·라벨 설명]` 블록으로 출력하되, "
    "도식 안의 텍스트(법인명·계약명·숫자)는 원문 그대로 포함할 것.\n"
    "5. 판독 불가한 부분만 [판독불가] 로 표기. 내용을 지어내지 말 것.\n"
    "6. 추출한 텍스트 외의 머리말·설명·인사말은 붙이지 말 것."
)

# 본문 페이지에 삽입된 *개별 그림* 설명용.
DESCRIBE_IMAGE_PROMPT = (
    "이것은 문서 본문에 삽입된 그림/도식/차트/사진입니다. 한국어로 간결하게 정리:\n"
    "1. 그림 유형 (사진 / 조직도 / 플로차트 / 그래프 / 다이어그램 등).\n"
    "2. 핵심 구성요소와 그들 사이의 관계 (화살표가 있으면 *방향* 포함).\n"
    "3. 그림 안에 보이는 모든 텍스트 라벨·숫자를 원문 그대로 인용.\n"
    "원본에 실제로 있는 정보만 사용. 추측 금지. 2~6줄로 요약."
)


def pdf_vision_enabled() -> bool:
    """PDF vision 활성화 여부. 상수 PDF_VISION_ENABLED."""
    return PDF_VISION_ENABLED


def _disable_thinking() -> bool:
    return (os.getenv("CUSTOM_LLM_DISABLE_THINKING", "") or "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _vlm_call(image_bytes: bytes, prompt: str, mime_type: str, max_tokens: int) -> str:
    """OpenAI 호환 /chat/completions vision 호출. 실패 시 빈 문자열."""
    cfg = resolve_llm_config()
    base_url = (cfg.get("base_url") or "").rstrip("/")
    api_key = cfg.get("api_key") or "not-needed"
    model = cfg.get("model") or ""
    if not base_url or not model:
        print("[vision] base_url/model 미설정 — skip")
        return ""

    b64 = base64.b64encode(image_bytes).decode("ascii")
    payload: Dict = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{b64}"}},
                ],
            }
        ],
        "temperature": 0.0,
        "max_tokens": max_tokens,
    }
    if cfg.get("provider") == "custom" and _disable_thinking():
        payload["chat_template_kwargs"] = {"enable_thinking": False}

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    if cfg.get("extra_headers"):
        headers.update(cfg["extra_headers"])

    try:
        with httpx.Client(timeout=VISION_TIMEOUT_SEC) as client:
            resp = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            return ""
        msg = choices[0].get("message") or {}
        content = msg.get("content") or msg.get("reasoning_content") or ""
        if isinstance(content, list):
            content = "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in content)
        return str(content).strip()
    except Exception as exc:
        print(f"[vision] 호출 실패: {exc}")
        return ""


def ocr_page_image(png_bytes: bytes) -> str:
    """스캔 페이지 PNG → 통합 OCR 텍스트(+도식 설명)."""
    return _vlm_call(png_bytes, UNIFIED_OCR_PROMPT, "image/png", max_tokens=OCR_MAX_TOKENS)


def describe_image(image_bytes: bytes, mime_type: str = "image/png") -> str:
    """본문 페이지에 삽입된 그림 → 설명 텍스트."""
    return _vlm_call(image_bytes, DESCRIBE_IMAGE_PROMPT, mime_type, max_tokens=DESCRIBE_MAX_TOKENS)


def run_parallel(tasks: List[Tuple[str, Callable[[], str]]]) -> Dict[str, str]:
    """(key, thunk) 목록을 ThreadPool 로 병렬 실행 → {key: result}.

    동시 호출 수는 상수 VISION_MAX_WORKERS 로 제한.
    """
    if not tasks:
        return {}
    max_workers = max(1, VISION_MAX_WORKERS)
    results: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        fut_map = {pool.submit(thunk): key for key, thunk in tasks}
        for fut in as_completed(fut_map):
            key = fut_map[fut]
            try:
                results[key] = fut.result()
            except Exception as exc:
                print(f"[vision] task {key} 실패: {exc}")
                results[key] = ""
    return results
