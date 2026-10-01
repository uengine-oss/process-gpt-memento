> **전체 설치 및 실행 방법은 메인 프로젝트를 참고하세요:** [ProcessGPT](https://github.com/uengine-oss/process-gpt)

# Process GPT Memento

FastAPI 기반 멀티테넌트 지식베이스 서비스입니다.  
업로드된 문서를 페이지 단위 원문으로 저장하고, 문서 카드와 폴더 카드로 **에이전트가 읽어
내려갈 지도**를 만듭니다. 벡터 인덱스(Chroma 또는 Qdrant)는 에이전트가 진입점을 잡는 검색 힌트로만
씁니다. 계약은 [docs/specs/knowledge-map.md](docs/specs/knowledge-map.md).

## 핵심 기능

- 입력 소스: Supabase Storage 업로드(지식베이스), Google Drive, 로컬 파일
- 문서 파싱: PDF, DOCX, PPTX, XLSX, TXT, HWP, HWPX → 페이지 단위 원문(`document_pages`)
- 문서 카드 / 폴더 카드: 무엇이 있고 무엇부터 읽을지 (`doc_cards.py`, `folder_cards.py`)
- 지도 API: 폴더 트리 · 폴더 열기 · 카탈로그 · 문서 grep · 페이지 읽기 (`folders.py`, `navigator.py`)
- 보조 검색: 벡터 유사도 검색(`/search`) — 실패해도 문서 상태에 영향 없음
- 이미지 추출 및 분석: PDF/DOCX/PPTX 및 단일 이미지(JPG/PNG/GIF/BMP/WEBP)
- LLM 호출 경로를 `litellm proxy`로 전환 가능 (`llm.py`)

## 문서

| 문서 | 내용 |
|---|---|
| [INTENT.md](INTENT.md) | 이 서비스가 왜 있고 무엇이 성공인가 |
| [AGENTS.md](AGENTS.md) | 작업 규칙, 명령, 코드 배치, 배포 |
| [docs/specs/](docs/specs/) | 계약 — 인제스트·카드·지도 API·호출처, 산출물 버킷 |
| [docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md) | 설계 근거와 실측 |

## 로컬 세팅

1. **Python 3.11+** 가상환경과 의존성

   ```bash
   python -m venv .venv
   .venv/Scripts/pip install -r requirements.txt      # Windows (POSIX: .venv/bin/pip)
   ```

2. **외부 도구** (Docker 이미지에는 들어 있다 — `Dockerfile`)
   - LibreOffice(`soffice`): DOC·PPTX 변환, DOCX 보기용 PDF
   - [rhwp](https://github.com/edwardkim/rhwp) 0.8.4: HWP→HWPX 변환과 한글 문서 보기용 PDF. PATH 에 두거나 `HWPX_RHWP` 로 지정
   - 한글 글꼴(나눔·Noto CJK): 보기용 PDF 렌더링

3. **환경 변수**: `cp .env.example .env` 후 채운다. 필수는 `SUPABASE_URL`·`SUPABASE_KEY` 와 LLM·임베딩 프로바이더 하나씩.
   전체 목록·기본값·뜻은 [docs/specs/configuration.md](docs/specs/configuration.md). 키는 커밋하지 않는다.
   - 사내망 모델(vLLM 등): `MEMENTO_LLM_PROVIDER=custom` + `CUSTOM_LLM_BASE_URL`(·`_API_KEY`·`_MODEL`)
   - litellm 프록시: `MEMENTO_LLM_PROVIDER=openai` + `OPENAI_LLM_BASE_URL`(프록시 주소)·`OPENAI_API_KEY`·`OPENAI_LLM_MODEL`
   - PDF 표 LLM 파싱은 기본 끔(`MEMENTO_TABLE_LLM`). 켤 때는 configuration.md 의 "PDF 표 LLM 파싱"을 먼저 읽는다.

4. **Supabase 마이그레이션**: `sql/` 의 SQL 을 SQL 에디터에서 한 번씩 실행한다(배포 환경에는 push 전에).
   `knowledge_doc_cards.sql`, `knowledge_folder_cards.sql`, `kb_mirror.sql`, `knowledge_files_path.sql`,
   `document_blocks.sql`, `document_sections.sql`, `glossary_terms.sql`, `perf_knowledge_indexes.sql`.

5. **실행·테스트**

   ```bash
   python main.py                               # http://localhost:8005
   .venv/Scripts/python.exe -m pytest -q        # Windows (POSIX: .venv/bin/python -m pytest -q)
   ```

   시작 로그의 "Provider configuration" 에 실제로 쓰는 LLM·임베딩 프로바이더와 모델이 찍힌다.

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
- `GET /glossary/terms`

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

- LLM·임베딩 초기화 실패: 시작 로그의 "Provider configuration" 에서 프로바이더·주소·모델을 확인한다. 키 이름은 프로바이더마다
  다르다(configuration.md).
- `documents` insert가 `embedding` 없이 실패하면 DB에서 `embedding` 컬럼이 여전히 non-null/vector 제약을 요구하는지 확인하세요. 즉시 우회가 필요하면 `SUPABASE_DUMMY_EMBEDDING_DIMENSIONS`를 기존 차원으로 맞추세요.
- HWP·HWPX 보기용 PDF 가 안 나오면 `rhwp` 가 PATH 에 있는지(또는 `HWPX_RHWP`) 확인한다.
- Drive 인증 오류 시 `/auth/google/url`로 OAuth URL을 먼저 발급하세요.
- 이미지 분석 실패 시 Supabase Storage 공개 URL 접근 가능 여부를 확인하세요.
