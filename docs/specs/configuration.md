# 설정

memento 의 설정은 **배포마다 환경 변수(.env)** 로 정한다. 호출하는 쪽(vue3·codex·agent-sdk·office-mcp)은 설정을
바꾸지 않는다 — 같은 문서를 여러 호출처가 함께 쓰므로, 저장되는 결과(파싱·색인)를 바꾸는 값은 서버가 정한다.
질의 한 번에만 영향을 주는 값(검색 개수·범위 등)만 요청 파라미터로 받는다.

- 시작할 때 `.env.example` 을 `.env` 로 복사해 채운다. 비밀값(★)은 커밋하지 않는다.
- 값이 비어 있으면 아래 기본값이다. 이 표에 없는 환경 변수는 memento 가 읽지 않는다(아래 "예전 이름" 제외).
- 환경 변수는 **배포마다 실제로 다른 값**(주소·키·모델·경로·스위치·서버 용량에 맞춘 동시 수)만 둔다. 배포와 상관없는
  튜닝 값(재시도 횟수, 창 크기, 컬렉션 이름, Qdrant 양자화 등)은 코드 상수다.
- 새 설정은 `MEMENTO_` 로 시작하고 이 표와 `.env.example` 을 같이 고친다.
- 운영(k8s) 배포 값은 `process-gpt-k8s/deployments/memento-deployment.yaml`.

## 필수

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `SUPABASE_URL` | — | Supabase 주소 |
| `SUPABASE_KEY` ★ | — | Supabase 서비스 키 |

## LLM

카드·섹션·그림 설명·OCR·표 파싱이 모두 이 LLM 을 쓴다. 프로바이더는 모두 OpenAI 호환 API 이고, 프로바이더 값은
기본 주소·모델과 특이사항(그림 지원 여부 등)만 정한다.

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_LLM_PROVIDER` | `openai` | `openai`(OpenAI·litellm 프록시) / `openrouter` / `custom`(vLLM 등 자체 서버) |
| `MEMENTO_LLM_BASE_URL` | openai `https://api.openai.com/v1`, openrouter `https://openrouter.ai/api/v1`, custom 필수 | 주소(`/v1` 까지) |
| `MEMENTO_LLM_API_KEY` ★ | — | 키(자체 서버는 비워도 된다) |
| `MEMENTO_LLM_MODEL` | openai `gpt-5.6-luna`, openrouter `openai/gpt-oss-120b`, custom `/models/openai/gpt-oss-120b` | 모델 |

샘플링 값(temperature·top_p·thinking 끄기 등)은 환경 변수가 아니라 `config/llm_sampling.json` 이 프로바이더·모델별로 정한다.
frentis 는 모델 규칙으로 thinking 을 끈다 — 운영처럼 `openai` 프로바이더(litellm)로 불러도 꺼진다.

## 임베딩

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_EMBEDDING_PROVIDER` | `openai` | `openai` / `openrouter` / `custom` / `self`(이 프로세스에서 sentence-transformers) |
| `MEMENTO_EMBEDDING_BASE_URL` | openai·openrouter 공식 주소, custom 필수 | 주소(`/v1` 까지) |
| `MEMENTO_EMBEDDING_API_KEY` ★ | — | 키 |
| `MEMENTO_EMBEDDING_MODEL` | openai `text-embedding-3-small`, openrouter `qwen/qwen3-embedding-4b`, custom `BAAI/bge-m3`, self `Qwen/Qwen3-Embedding-0.6B` | 모델 |
| `MEMENTO_EMBEDDING_DEVICE` | `cuda` | self 의 장치 |
| `MEMENTO_EMBEDDING_BATCH_SIZE` | `8` | 한 번에 보내는 청크 수(임베딩 서버 한도에 맞춘다) |

임베딩 모델이나 차원을 바꾸면 벡터 인덱스를 다시 만들어야 한다(`QDRANT_VECTOR_SIZE` 도 맞춘다).

## 파서

HWP·HWPX·DOC·DOCX·PDF 는 [process-gpt-document-parser](https://github.com/uengine-oss/process-gpt-document-parser) 로 읽는다.
표 구조는 규칙으로 읽고, 그림 설명·스캔 쪽 OCR 만 아래 LLM 을 부른다. 측정은 그 저장소 `docs/BENCHMARKS.md`.

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_PDF_VISION_WORKERS` | `4` | 한 문서의 그림 설명·OCR 동시 호출 수(LLM 서버 용량에 맞춘다) |
| `MEMENTO_IMAGE_ANALYSIS` | 프로바이더가 그림을 받으면 켬 | 추출 이미지·이미지 파일 재분석. 문서 속 그림 설명·스캔 쪽 OCR 은 이 값과 무관하게 돈다 |
| `HWPX_RHWP` | PATH 의 `rhwp` | 한글 문서 보기용 PDF 를 만드는 rhwp 실행 파일 |

## 벡터 인덱스

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `VECTOR_BACKEND` | `chroma` | `chroma` / `qdrant`(대용량. 이관은 `scripts/migrate_chroma_to_qdrant.py`) |
| `CHROMA_SERVER_HOST`, `CHROMA_SERVER_PORT` | — / `8000` | 비우면 프로세스 안 Chroma, 주면 Chroma 서버 |
| `CHROMA_PERSIST_DIRECTORY` | `./chroma_db` | 프로세스 안 Chroma 저장 폴더 |
| `QDRANT_URL` 또는 `QDRANT_HOST`·`QDRANT_PORT` | — / `127.0.0.1`·`6333` | Qdrant 서버 |
| `QDRANT_API_KEY` ★ | — | Qdrant 키 |
| `QDRANT_VECTOR_SIZE` | `1536` | 임베딩 차원(컬렉션을 만들 때만) |
| `QDRANT_ON_DISK` | `true` | 원본 벡터·HNSW 를 디스크(mmap)에. 스토리지가 NVMe 가 아니면 false 를 검토 |

컬렉션 이름(`documents`·`kb_sections`), 양자화(`int8`), 재채점 배수(2.0), 탐색 폭(100)은 `app/core/config.py` 상수다
(근거: `benchmark/REPORT_chroma_to_qdrant.md`).

## 인제스트

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `MEMENTO_INGEST_ENABLED` | `true` | 인제스트 큐 워커. 대량 삭제 등으로 잠시 멈출 때 false |
| `MEMENTO_INGEST_CONCURRENCY` / `_MAX_CONCURRENCY` | `4` / `12` | 동시 인제스트 기본·상한 |
| `MEMENTO_INGEST_JOB_TIMEOUT` | `900` | 파일 하나 제한 시간(초). 넘으면 끊고 다시 시도 |
| `MEMENTO_VISION_MAX_INFLIGHT` | `8` | 추출 이미지 재분석의 프로세스 전체 동시 호출 상한 |
| `MEMENTO_DRIVE_FOLDER_ID` | 코드 기본값 | Google 드라이브 처리 때 함께 쓰는 폴더 |

## 변환본·산출물·기타

| 이름 | 기본값 | 뜻 |
|---|---|---|
| `RENDITION_PREWARM` | `true` | 인덱싱 뒤 변환본(보기용 PDF)을 미리 만듦 |
| `RENDITION_CACHE_DIR` | `.cache/renditions` | 변환본 로컬 캐시 |
| `ARTIFACT_BUCKET` | `artifacts` | 에이전트 산출물 비공개 버킷 |
| `ROBO_GLOSSARY_API_BASE_URL` | `http://127.0.0.1:5504/robo` | 용어사전 서비스 |
| `MEMENTO_THREAD_POOL` | `64` | asyncio 기본 스레드 풀 크기(0 이면 파이썬 기본값) |
| `MEMENTO_TRACEMALLOC` | `0` | 메모리 추적(진단용) |

## 예전 이름

LLM·임베딩의 예전 이름은 아직 읽지만, 쓰이면 시작 로그에 "예전 이름이다 — 새 이름으로 바꾼다" 경고를 낸다. 운영 배포를
새 이름으로 옮긴 뒤 코드에서 지운다(`app/core/config.py` 의 `legacy_*`). 예전 이름은 고른 프로바이더의 것만 읽는다.

| 새 이름 | 예전 이름(프로바이더) |
|---|---|
| `MEMENTO_LLM_BASE_URL` | `OPENAI_LLM_BASE_URL`·`LLM_BASE_URL`·`LLM_PROXY_URL`(openai), `OPENROUTER_LLM_BASE_URL`·`OPENROUTER_BASE_URL`(openrouter), `CUSTOM_LLM_BASE_URL`(custom) |
| `MEMENTO_LLM_API_KEY` | `OPENAI_LLM_API_KEY`·`LLM_API_KEY`·`LLM_PROXY_API_KEY`·`OPENAI_API_KEY`(openai), `OPENROUTER_API_KEY`·`OPENROUTER_LLM_API_KEY`(openrouter), `CUSTOM_LLM_API_KEY`(custom) |
| `MEMENTO_LLM_MODEL` | `OPENAI_LLM_MODEL`·`LLM_MODEL`(openai), `OPENROUTER_LLM_MODEL`(openrouter), `CUSTOM_LLM_MODEL`(custom) |
| `MEMENTO_EMBEDDING_BASE_URL` | `OPENAI_EMBEDDING_BASE_URL`·`EMBEDDING_BASE_URL`·`LLM_PROXY_URL`(openai), `OPENROUTER_EMBEDDING_BASE_URL`·`OPENROUTER_BASE_URL`(openrouter), `CUSTOM_EMBEDDING_BASE_URL`(custom) |
| `MEMENTO_EMBEDDING_API_KEY` | `OPENAI_EMBEDDING_API_KEY`·`EMBEDDING_API_KEY`·`LLM_PROXY_API_KEY`·`OPENAI_API_KEY`(openai), `OPENROUTER_API_KEY`·`OPENROUTER_EMBEDDING_API_KEY`(openrouter), `CUSTOM_EMBEDDING_API_KEY`(custom) |
| `MEMENTO_EMBEDDING_MODEL` | `OPENAI_EMBEDDING_MODEL`·`LLM_EMBEDDING_MODEL`(openai), `OPENROUTER_EMBEDDING_MODEL`(openrouter), `CUSTOM_EMBEDDING_MODEL`(custom), `SELF_EMBEDDING_MODEL`(self) |
| `MEMENTO_EMBEDDING_DEVICE` | `SELF_EMBEDDING_DEVICE` |
| `MEMENTO_EMBEDDING_BATCH_SIZE` | `EMBEDDING_BATCH_SIZE` |
| (코드 상수) | `QDRANT_COLLECTION_NAME`, `CHROMA_COLLECTION_NAME` — 다른 이름을 쓰던 환경이 받자마자 다른 컬렉션을 보지 않게 아직 읽는다 |

운영 k8s 가 지금 넣는 `LLM_BASE_URL`·`LLM_MODEL`·`OPENAI_API_KEY`·`EMBEDDING_BASE_URL`·`OPENAI_EMBEDDING_MODEL` 은
`MEMENTO_LLM_BASE_URL`·`MEMENTO_LLM_MODEL`·`MEMENTO_LLM_API_KEY`(+ `MEMENTO_EMBEDDING_API_KEY`)·`MEMENTO_EMBEDDING_BASE_URL`·
`MEMENTO_EMBEDDING_MODEL` 로 옮긴다. `OPENAI_API_KEY` 는 LLM 과 임베딩 키를 함께 채우고 있었으니 둘 다 넣는다.

그 밖에 지운 설정(코드 상수가 됐다, `config.IGNORED_ENV`): `CUSTOM_LLM_DISABLE_THINKING`(`llm_sampling.json` 이 custom 에 이미 싣는다),
`OPENROUTER_HTTP_REFERER`·`OPENROUTER_APP_TITLE`, `EMBEDDING_TIMEOUT_SEC`, `MEMENTO_EMBED_MAX_RETRIES`,
`KB_SECTION_COLLECTION`, `QDRANT_QUANTIZATION`·`QDRANT_SEARCH_OVERSAMPLING`·
`QDRANT_HNSW_EF`, `SUPABASE_WRITE_EMBEDDING`·`SUPABASE_DUMMY_EMBEDDING_DIMENSIONS`, `MEMENTO_INGEST_MAX_RETRIES`·`_QUEUE_MAX`·
`_LEASE_SEC`·`_SWEEP_SEC`, `MEMENTO_VISION_MAX_RETRIES`, `KB_CARD_CONCURRENCY`·`KB_CARD_WINDOW_CHARS`·`KB_CARD_MAX_WINDOWS`,
`KB_SECTION_MAX_CHARS`·`KB_SECTION_EMBED_CHARS`, `RENDITION_PREWARM_CONCURRENCY`, `ARTIFACT_URL_TTL_SECONDS`,
`ROBO_GLOSSARY_TIMEOUT_SEC`, `MIGRATE_BATCH_SIZE`(→ `--batch-size`). `.env` 에 남아 있으면 무시하고, 시작 로그에
"더 이상 읽지 않는다" 경고를 낸다.
