# 설정

memento 의 설정은 **배포마다 환경 변수(.env)** 로 정한다. 호출하는 쪽(vue3·codex·agent-sdk·office-mcp)은 설정을
바꾸지 않는다 — 같은 문서를 여러 호출처가 함께 쓰므로, 저장되는 결과(파싱·색인)를 바꾸는 값은 서버가 정한다.
질의 한 번에만 영향을 주는 값(검색 개수·범위 등)만 요청 파라미터로 받는다.

- 시작할 때 `.env.example` 을 `.env` 로 복사해 채운다. 비밀값(★)은 커밋하지 않는다.
- 값이 비어 있으면 아래 기본값이다. 이 표에 없는 환경 변수는 memento 가 읽지 않는다.
- 새 설정은 `MEMENTO_` 로 시작하고 여기 표에 올린다. 코드에서 읽는 곳은 `app/core/config.py`·
  `app/plugins/parsers/config.py` 등 표의 "읽는 곳".
- 운영(k8s) 배포 값은 `process-gpt-k8s/deployments/memento-deployment.yaml`.

## 필수

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `SUPABASE_URL` | — | Supabase 주소 |
| `SUPABASE_KEY` ★ | — | Supabase 서비스 키 |

## LLM

카드·섹션·그림 설명·OCR·표 파싱이 모두 이 LLM 을 쓴다. 프로바이더를 고르고, 그 프로바이더의 주소·키·모델을 준다.
목록의 이름은 앞에 있는 것이 이긴다.

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_LLM_PROVIDER` | `openai` | `openai`(OpenAI 호환: OpenAI·litellm 프록시 등) / `openrouter` / `custom`(vLLM 등 자체 서버) |
| `OPENAI_LLM_BASE_URL`, `LLM_BASE_URL`, `LLM_PROXY_URL` | `https://api.openai.com/v1` | openai 주소. 운영은 litellm 프록시 |
| `OPENAI_LLM_API_KEY`, `LLM_API_KEY`, `LLM_PROXY_API_KEY`, `OPENAI_API_KEY` ★ | — | openai 키 |
| `OPENAI_LLM_MODEL`, `LLM_MODEL` | `gpt-5.6-luna` | openai 모델 |
| `OPENROUTER_LLM_BASE_URL`, `OPENROUTER_BASE_URL` | `https://openrouter.ai/api/v1` | openrouter 주소 |
| `OPENROUTER_API_KEY`, `OPENROUTER_LLM_API_KEY` ★ | — | openrouter 키 |
| `OPENROUTER_LLM_MODEL` | `openai/gpt-oss-120b` | openrouter 모델 |
| `OPENROUTER_HTTP_REFERER`, `OPENROUTER_APP_TITLE` | — | openrouter 요청 머리 |
| `CUSTOM_LLM_BASE_URL` | — (custom 이면 필수) | 자체 서버 주소 |
| `CUSTOM_LLM_API_KEY` ★ | — | 자체 서버 키(없으면 빈 키로 부른다) |
| `CUSTOM_LLM_MODEL` | `/models/openai/gpt-oss-120b` | 자체 서버 모델 |
| `CUSTOM_LLM_DISABLE_THINKING` | 끔 | custom 그림 호출에 `enable_thinking=false` 를 싣는다 |

샘플링 값(temperature·top_p·thinking 끄기 등)은 환경 변수가 아니라 `config/llm_sampling.json` 이 모델별로 정한다.

### 역할별 모델

기본 LLM 과 다른 모델을 쓰고 싶은 역할만 덮는다. 비우면 기본 LLM 을 쓴다. 지금 역할은 `table`(PDF 표 파싱)뿐이다.

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_TABLE_LLM_PROVIDER` | 기본 LLM 프로바이더 | 표 파싱 프로바이더. 그 프로바이더의 주소·키 환경 변수를 쓴다 |
| `MEMENTO_TABLE_LLM_MODEL` | 그 프로바이더의 모델 | 표 파싱 모델(그림을 받는 모델이어야 한다) |

## 임베딩

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_EMBEDDING_PROVIDER` | `openai` | `openai` / `openrouter` / `custom` / `self`(이 프로세스에서 sentence-transformers) |
| `OPENAI_EMBEDDING_BASE_URL`, `EMBEDDING_BASE_URL`, `LLM_PROXY_URL` | `https://api.openai.com/v1` | openai 주소 |
| `OPENAI_EMBEDDING_API_KEY`, `EMBEDDING_API_KEY`, `LLM_PROXY_API_KEY`, `OPENAI_API_KEY` ★ | — | openai 키 |
| `OPENAI_EMBEDDING_MODEL`, `LLM_EMBEDDING_MODEL` | `text-embedding-3-small` | openai 모델 |
| `OPENROUTER_EMBEDDING_BASE_URL`, `OPENROUTER_BASE_URL` | `https://openrouter.ai/api/v1` | openrouter 주소 |
| `OPENROUTER_API_KEY`, `OPENROUTER_EMBEDDING_API_KEY` ★ | — | openrouter 키 |
| `OPENROUTER_EMBEDDING_MODEL` | `qwen/qwen3-embedding-4b` | openrouter 모델 |
| `CUSTOM_EMBEDDING_BASE_URL` | — (custom 이면 필수) | 자체 서버 주소 |
| `CUSTOM_EMBEDDING_API_KEY` ★ | — | 자체 서버 키 |
| `CUSTOM_EMBEDDING_MODEL` | `BAAI/bge-m3` | 자체 서버 모델 |
| `SELF_EMBEDDING_MODEL` | `Qwen/Qwen3-Embedding-0.6B` | self 모델 |
| `SELF_EMBEDDING_DEVICE` | `cuda` | self 장치 |
| `EMBEDDING_TIMEOUT_SEC` | `180` | 임베딩 호출 제한 시간(초) |
| `EMBEDDING_BATCH_SIZE` | `8` | 한 번에 보내는 청크 수 |
| `MEMENTO_EMBED_MAX_RETRIES` | `3` | 임베딩 재시도 |

임베딩 모델이나 차원을 바꾸면 벡터 인덱스를 다시 만들어야 한다.

## 벡터 인덱스

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `VECTOR_BACKEND` | `chroma` | `chroma` / `qdrant`(대용량) |
| `CHROMA_SERVER_HOST`, `CHROMA_SERVER_PORT` | — / `8000` | 비우면 프로세스 안 Chroma, 주면 Chroma 서버 |
| `CHROMA_PERSIST_DIRECTORY` | `./chroma_db` | 프로세스 안 Chroma 저장 폴더 |
| `CHROMA_COLLECTION_NAME` | `documents` | 청크 컬렉션 |
| `QDRANT_URL` 또는 `QDRANT_HOST`·`QDRANT_PORT` | — / `127.0.0.1`·`6333` | Qdrant 서버 |
| `QDRANT_API_KEY` ★ | — | Qdrant 키 |
| `QDRANT_COLLECTION_NAME` | `documents` | 청크 컬렉션 |
| `QDRANT_VECTOR_SIZE` | `1536` | 임베딩 차원 |
| `QDRANT_ON_DISK` | `true` | 원본 벡터·HNSW 를 디스크(mmap)에 |
| `QDRANT_QUANTIZATION` | `int8` | `int8` / `binary` / `none` |
| `QDRANT_SEARCH_OVERSAMPLING` | `2.0` | 양자화 후보를 몇 배로 뽑아 재채점할지 |
| `QDRANT_HNSW_EF` | `100` | 검색 탐색 폭 |
| `KB_SECTION_COLLECTION` | `kb_sections` | 섹션 컬렉션 |
| `SUPABASE_WRITE_EMBEDDING` | `false` | 청크 벡터를 Supabase 에도 쓸지 |
| `SUPABASE_DUMMY_EMBEDDING_DIMENSIONS` | `1536` | 위가 꺼졌을 때 Supabase 에 넣는 빈 벡터 차원 |

## 파서

근거·측정은 `docs/DESIGN_NOTES.md` "파서 점검"·"선 없는 하위 행"·"PDF 표: 규칙 대 LLM".

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `PDF_STRATEGY` | `pymupdf_region` | PDF 파서: `pymupdf_region` / `pymupdf` / `pdfplumber` |
| `PDF_UNRULED_TABLES` | `true` | 괘선 없는 표를 글 배치로 되살림 |
| `PDF_SPLIT_SUBROWS` | `true` | 표 안에서 가로선 없이 묶인 하위 행을 나눔 |
| `MEMENTO_TABLE_LLM` | `false` | **PDF 표를 LLM 으로 다시 읽음**(아래). 표마다 10초 안팎 |
| `MEMENTO_PDF_VISION_WORKERS` | `4` | PDF 그림 설명·OCR·표 파싱 동시 호출 수 |
| `MEMENTO_IMAGE_ANALYSIS` | 프로바이더가 그림을 받으면 켬 | 추출 이미지·이미지 파일 재분석. PDF 쪽·그림 영역 VLM 은 이 값과 무관하게 돈다 |
| `HWPX_RHWP` | PATH 의 `rhwp` | HWP→HWPX 변환·변환본에 쓰는 rhwp 실행 파일 |

### PDF 표 LLM 파싱 (`MEMENTO_TABLE_LLM`)

켜면 PDF 를 읽을 때 `find_tables` 가 찾은 표마다 영역 그림과 그 영역의 PDF 글자를 LLM(`table` 역할 모델)에 주고
마크다운 표를 받는다. 받은 표의 숫자가 모두 그 영역의 PDF 글자에 있을 때만 바꾸고, 아니면 규칙으로 만든 표를 둔다
(쪽 메타데이터 `table_llm`·`table_llm_dropped`). 선이 없어 `find_tables` 가 못 찾은 표는 대상이 아니다.

- 인제스트 큐 안에서 돈다. 표가 많은 문서는 인제스트가 그만큼 길어진다(`MEMENTO_INGEST_JOB_TIMEOUT` 확인).
- 켜면 파서 버전에 모델이 붙는다(`2026-10-01.subrows+table-llm:<모델>`). 켜거나 끄거나 모델을 바꾼 뒤
  `python -m scripts.reindex_stale <tenant> --ext pdf` 로 PDF 만 다시 인덱싱한다.
- 그림을 받는 모델이어야 한다. 폐쇄망은 사내 모델(frentis)로도 효과가 있다(DESIGN_NOTES).

## 인제스트·카드·섹션

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_INGEST_ENABLED` | `true` | 인제스트 큐 워커 |
| `MEMENTO_INGEST_CONCURRENCY` / `_MAX_CONCURRENCY` | `4` / `12` | 동시 인제스트 기본·상한 |
| `MEMENTO_INGEST_MAX_RETRIES` | `3` | 실패 재시도 |
| `MEMENTO_INGEST_QUEUE_MAX` | `20000` | 큐 길이 상한 |
| `MEMENTO_INGEST_LEASE_SEC` | `1800` | 처리 중으로 이만큼(초) 멈춘 작업을 다시 줍는다 |
| `MEMENTO_INGEST_JOB_TIMEOUT` | `900` | 작업 하나 제한 시간(초) |
| `MEMENTO_INGEST_SWEEP_SEC` | `20` | pending 을 다시 줍는 주기(초) |
| `MEMENTO_VISION_MAX_INFLIGHT` / `_MAX_RETRIES` | `8` / `2` | 이미지 재분석 동시 호출·재시도 |
| `KB_CARD_CONCURRENCY` | `6` | 문서 카드 동시 생성 |
| `KB_CARD_WINDOW_CHARS` / `KB_CARD_MAX_WINDOWS` | `12000` / `16` | 카드 생성에 보는 글 창 크기·수 |
| `KB_SECTION_MAX_CHARS` | `8000` | 섹션 하나의 최대 글자 |
| `KB_SECTION_EMBED_CHARS` | `4000` | 섹션 벡터에 넣는 글자 |
| `MEMENTO_DRIVE_FOLDER_ID` | 코드 기본값 | Google 드라이브 처리 때 함께 쓰는 폴더 |

## 변환본·산출물·기타

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `RENDITION_PREWARM` | `true` | 인덱싱 뒤 변환본(보기용 PDF)을 미리 만듦 |
| `RENDITION_PREWARM_CONCURRENCY` | `1` | 미리 만들기 동시 수 |
| `RENDITION_CACHE_DIR` | `.cache/renditions` | 변환본 로컬 캐시 |
| `ARTIFACT_BUCKET` | `artifacts` | 에이전트 산출물 비공개 버킷 |
| `ARTIFACT_URL_TTL_SECONDS` | `3600` | 산출물 서명 주소 수명(초) |
| `ROBO_GLOSSARY_API_BASE_URL` | — | 용어사전 서비스(비우면 안 쓴다) |
| `ROBO_GLOSSARY_TIMEOUT_SEC` | `5` | 용어사전 호출 제한 시간 |
| `MEMENTO_THREAD_POOL` | `64` | asyncio 기본 스레드 풀 크기(0 이면 파이썬 기본값) |
| `MEMENTO_TRACEMALLOC` | `0` | 메모리 추적(진단용) |
| `MIGRATE_BATCH_SIZE` | `500` | `scripts/migrate_chroma_to_qdrant.py` 배치 |
