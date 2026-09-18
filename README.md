> **전체 설치 및 실행 방법은 메인 프로젝트를 참고하세요:** [ProcessGPT](https://github.com/uengine-oss/process-gpt)

# Process GPT Memento

FastAPI 기반 멀티테넌트 지식베이스 서비스입니다.  
업로드된 문서를 페이지 단위 원문으로 저장하고, 문서 카드와 폴더 카드로 **에이전트가 읽어
내려갈 지도**를 만듭니다. 벡터 인덱스(Chroma)는 에이전트가 진입점을 잡는 검색 힌트로만
씁니다. 설계 근거는 [docs/knowledge-map.md](docs/knowledge-map.md).

## 핵심 기능

- 입력 소스: Supabase Storage 업로드(지식베이스), Google Drive, 로컬 파일
- 문서 파싱: PDF, DOCX, PPTX, XLSX, TXT, HWP, HWPX → 페이지 단위 원문(`document_pages`)
- 문서 카드 / 폴더 카드: 무엇이 있고 무엇부터 읽을지 (`doc_cards.py`, `folder_cards.py`)
- 지도 API: 폴더 트리 · 폴더 열기 · 카탈로그 · 문서 grep · 페이지 읽기 (`folders.py`, `navigator.py`)
- 보조 검색: Chroma 유사도 검색(`/search`) — 실패해도 문서 상태에 영향 없음
- 이미지 추출 및 분석: PDF/DOCX/PPTX 및 단일 이미지(JPG/PNG/GIF/BMP/WEBP)
- LLM 호출 경로를 `litellm proxy`로 전환 가능 (`llm.py`)

## 문서 카드 (`knowledge_doc_cards`)

에이전트가 폴더 수천 건에서 문서를 고르려면 "무엇에 대한 문서인가"가 아니라 **"이 문서를
열어야 하는가"** 를 답하는 메타데이터가 필요하다. 기존 `knowledge_files.doc_card` 의
abstract 는 앞 3쪽 + 뒤 1쪽만 보고 만든 한 줄이라 300쪽 문서에서는 후자를 답하지 못했다.

`app/services/doc_cards.py` 는 문서 전문을 **길이 기반 슬라이딩 윈도우** 로 잘라 순서대로
읽으며 카드를 갱신한다. 목차·헤딩·페이지 구조를 가정하지 않으므로 공문·엑셀·메일 뭉치가
같은 경로를 탄다. 사실은 합집합으로 누적되고 요약만 교체되며, 예산(`KB_CARD_MAX_WINDOWS`,
기본 16)을 넘으면 앞부분만 읽는 대신 문서 전체에 고르게 흩어 읽는다.

카드 필드: `title`(본문 기준) · `summary` · `doc_type` · `distinguishers`(옆 문서와 구별하는
사실) · `topics` · `entities` · `keywords` · `language` · `answers_questions`(리트리벌 표면) ·
`coverage`(얼마나 읽었는가).

- 카드를 만들 때 같은 폴더의 다른 문서 제목을 함께 보여 준다. 동일 골격의 사업 문서가
  여러 벌이면 문서 하나만 보고 쓴 요약은 서로 같아지기 때문이다.
- 카드 생성은 인제스트를 막지 않는다 — 페이지 저장 뒤 백그라운드로 돌고
  `status`(pending/done/failed/empty)로 진행을 드러낸다.
- 같은 내용(`content_sha256`)의 문서를 다시 올리면 카드를 재사용한다.
- 텍스트 레이어가 없는 문서는 카드 대신 `has_text=false` 로 남는다. "자료에 없음" 과
  "읽을 수 없음" 은 다른 결론이다.

## 폴더 카드 (`knowledge_folder_cards`)

폴더가 지도의 단위다. 자식 문서 카드와 하위 폴더 카드를 bottom-up 으로 모아 폴더당 LLM
1회로 만든다. 필드: `summary` · `topics` · `reading_guide`(어떤 질문이면 무엇부터 열지) ·
`start_with`(맥락을 가장 빨리 잡는 문서) · `answers_questions` · `cards`(직속 문서 카드
준비 상태) · 결정론 필드(문서 수·기간·종류·후보 엔티티).

`/folders/tree` 와 `/folders/open` 이 이 카드와 문서별 준비 상태(`ready/pending/failed/no_text`)를
돌려주며, 관리 화면과 에이전트가 같은 응답을 본다.

마이그레이션(모두 멱등, Supabase SQL 에디터에서 1회): `sql/knowledge_doc_cards.sql`,
`sql/knowledge_folder_cards.sql`, `sql/kb_mirror.sql`. `/folders/tree`·`/folders/open` 이
`knowledge_files.has_text`/`page_count`(kb_mirror.sql) 를 읽으므로 이 셋은 지도 API 의 전제다.
`kb_mirror.sql` 의 `kb_page_text` RPC 는 codex 미러가 파일별 전문을 배치로 가져오는 데 쓴다.

## 아키텍처 개요

- API 엔트리포인트: `main.py` (기본 포트 `8005`)
- 문서 로딩/청킹: `document_loader.py`
- RAG 체인/이미지 분석: `rag_chain.py`
- 저장/검색 브리지: `vector_store.py` (`Supabase documents` + `Chroma`)
- LLM 팩토리(프록시 라우팅): `llm.py`

## 환경 변수

`.env` 파일 예시:

```env
# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_service_or_anon_key

# LLM Proxy (권장)
LLM_PROXY_URL=http://litellm-proxy:4000
LLM_PROXY_API_KEY=your_virtual_key
LLM_MODEL=gpt-4o
LLM_EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_BASE_URL=
EMBEDDING_TIMEOUT_SEC=60
CHROMA_PERSIST_DIRECTORY=./chroma_db
CHROMA_COLLECTION_NAME=documents
SUPABASE_WRITE_EMBEDDING=false
SUPABASE_DUMMY_EMBEDDING_DIMENSIONS=1536

# Fallback/OpenAI (일부 모듈에서 여전히 사용)
OPENAI_API_KEY=your_openai_api_key

# Google Drive 처리 관련
MEMENTO_DRIVE_FOLDER_ID=optional_extra_folder_id
```

참고:
- `rag_chain.py`의 LLM 호출은 `llm.py:create_llm()`을 사용합니다.
- 임베딩은 `llm.py:create_embeddings()`를 통해 `EMBEDDING_BASE_URL`이 있으면 이를 우선 사용하고, 없으면 `LLM_PROXY_URL`을 사용합니다. 모델은 `LLM_EMBEDDING_MODEL`을 사용합니다.
- 임베딩 클라이언트는 OpenAI 호환 `/embeddings` 응답의 `data[].embedding` 또는 `embeddings` 형식을 모두 허용합니다.
- 검색은 Chroma에서 수행한 뒤, hit metadata의 `document_row_id`로 Supabase `documents` 원문을 다시 조회합니다.
- `SUPABASE_WRITE_EMBEDDING=false`가 기본값이며, 이 경우 Supabase `documents.embedding` 컬럼은 유지하더라도 쓰지 않습니다.
- 레거시 스키마가 `embedding` non-null/vector 제약을 아직 요구하면 `SUPABASE_DUMMY_EMBEDDING_DIMENSIONS` 길이의 zero vector를 저장해 원문 insert만 통과시킵니다.
- 일부 섹션 타이틀 생성 로직은 현재 `OPENAI_API_KEY`를 사용합니다.

## 설치

```bash
pip install -r requirements.txt
```

또는 프로젝트가 `pyproject.toml` 기반이라면 사용 중인 패키지 매니저(`uv`, `pip`)에 맞춰 설치하세요.

## 실행

```bash
python main.py
```

기본 실행 주소:
- `http://localhost:8005`

## 주요 API

### 지식베이스 관리 (`/knowledge/*`)

- `POST /knowledge/files/upload` — 스토리지 저장 + `pending` 등록. 인제스트는 백그라운드 워커.
- `GET /knowledge/ingest/status` — 테넌트 상태별 카운트 + 큐 스냅샷
- `POST /knowledge/files/reindex` — 원본을 다시 받아 전체 재처리
- `POST /knowledge/files/resummarize` — 저장된 페이지로 문서 카드만 다시 생성
- `GET /knowledge/files/url` · `DELETE /knowledge/files` · `GET /knowledge/files/check-hash`
- `GET|POST|DELETE /knowledge/folders`, `POST /knowledge/folders/rename`
- `POST /knowledge/folders/refresh-cards` — 영향받은 폴더(+조상) 카드만 재생성
- `POST /knowledge/folders/build-cards` — 테넌트 전체 백필(관리자)

### 지도 (에이전트와 관리 화면이 같이 씀)

- `GET /folders/tree` — 폴더 골격 + 폴더 카드 + 준비 상태
- `GET /folders/open` — 한 폴더의 하위 폴더 + 문서 카드. `include_refs=true` 면 관리 필드 포함
- `GET /folders/card` — 폴더 카드 1건
- `GET /catalog` — 선택 자료의 문서 카드 목록
- `GET /document/grep` · `GET /document/page` · `GET /document/raw`
- `GET /glossary/inline` · `GET /glossary/terms`

### 보조 검색

- `GET /search` — 엄격한 벡터 top-k. `file_ids` / `folder_paths` 로 스코프
- `GET /retrieve` — 레거시 호출자용(agent-sdk). 신규 코드는 `/search`
- `GET /documents/list` · `GET /documents/full-text`

### 처리

- `POST /process` — `storage_type=local|drive|storage`
- `POST /process-output` — 워크아이템 산출물 DOCX 생성 + Drive 업로드
- `GET /process/drive/status`

### 업로드

- `POST /save-to-storage`  
  - 파일 업로드 + 처리 + 벡터 저장
- `POST /save-to-drive`  
  - 파일을 Google Drive에 업로드

### 인증

- `GET /auth/google/url`
- `GET /auth/google/status`
- `POST /auth/google/save-token`
- `POST /auth/google/callback`

## 사용 예시

### 1) 문서 처리 요청

```bash
curl -X POST "http://localhost:8005/process" \
  -H "Content-Type: application/json" \
  -d '{
    "storage_type": "drive",
    "tenant_id": "localhost"
  }'
```

### 2) 지도 열기

```bash
curl "http://localhost:8005/folders/tree?tenant_id=localhost&depth=2"
curl "http://localhost:8005/folders/open?tenant_id=localhost&folder_path=A사업/계약"
```

### 3) 보조 검색

```bash
curl "http://localhost:8005/search?query=계약금액&tenant_id=localhost&top_k=5"
```

## 지원 파일 형식

| 형식 | 텍스트 추출 | 문서 내 이미지 추출 |
|------|-------------|----------------------|
| PDF  | ✅ | ✅ |
| DOCX | ✅ | ✅ |
| PPTX | ✅ | ✅ |
| XLSX | ✅ | ❌ |
| TXT  | ✅ | ❌ |
| HWP  | ✅ | ❌ |
| HWPX | ✅ | ❌ |
| JPG/PNG/GIF/BMP/WEBP | (단일 이미지 문서로 처리) | - |

## 저장소 스키마 참고

아래 저장소가 필요합니다.

- Supabase 테이블 `documents` (원문 `content`, `metadata`, 필요 시 `embedding` 컬럼 유지 가능)
- `document_images` (추출 이미지 메타)
- `processed_files` (중복 처리 방지)
- 로컬 Chroma persistence directory (`CHROMA_PERSIST_DIRECTORY`)

참고:
- 더 이상 `match_documents` RPC는 필수 전제 조건이 아닙니다.
- `documents.embedding` 컬럼이 남아 있어도 되지만, `SUPABASE_WRITE_EMBEDDING=false`일 때는 null 허용 또는 비활성화 상태여야 합니다.
- 컬럼 제약을 바로 바꾸기 어렵다면 `SUPABASE_DUMMY_EMBEDDING_DIMENSIONS`를 기존 vector 차원(예: `1536`)으로 맞춰 레거시 컬럼만 유지할 수 있습니다.

## 문제 해결

- `LLM_PROXY_API_KEY` 또는 `OPENAI_API_KEY`가 없으면 RAG LLM 초기화가 실패할 수 있습니다.
- `OPENAI_API_KEY`가 없으면 임베딩/일부 섹션 타이틀 생성이 실패할 수 있습니다.
- `documents` insert가 `embedding` 없이 실패하면 DB에서 `embedding` 컬럼이 여전히 non-null/vector 제약을 요구하는지 확인하세요. 즉시 우회가 필요하면 `SUPABASE_DUMMY_EMBEDDING_DIMENSIONS`를 기존 차원으로 맞추세요.
- Drive 인증 오류 시 `/auth/google/url`로 OAuth URL을 먼저 발급하세요.
- 이미지 분석 실패 시 Supabase Storage 공개 URL 접근 가능 여부를 확인하세요.
- HWP/HWPX 파서(`extract-hwp`)가 없거나 실패하면 PDF 변환 폴백을 시도합니다. 이 경로를 쓰려면 서버/컨테이너에 `LibreOffice(soffice)`가 설치되어 있어야 합니다.