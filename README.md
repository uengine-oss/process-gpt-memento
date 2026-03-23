# Process GPT Memento

FastAPI 기반 멀티테넌트 문서 처리/검색(RAG) 서비스입니다.  
Google Drive, Supabase Storage, 로컬 파일에서 문서를 수집하고 청킹/임베딩/벡터검색을 제공합니다.

## 핵심 기능

- 다양한 입력 소스 처리: `drive`, `storage`, `local`, `database`
- 문서 파싱/청킹: PDF, DOCX, PPTX, XLSX, TXT, HWP, HWPX
- 이미지 추출 및 분석: PDF/DOCX/PPTX 및 단일 이미지(JPG/PNG/GIF/BMP/WEBP)
- Supabase 기반 벡터 저장/유사도 검색
- Google OAuth 기반 테넌트별 Drive 접근
- LLM 호출 경로를 `litellm proxy`로 전환 가능 (`llm.py`)

## 아키텍처 개요

- API 엔트리포인트: `main.py` (기본 포트 `8005`)
- 문서 로딩/청킹: `document_loader.py`
- RAG 체인/이미지 분석: `rag_chain.py`
- 벡터 저장소: `vector_store.py`
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

# Fallback/OpenAI (일부 모듈에서 여전히 사용)
OPENAI_API_KEY=your_openai_api_key

# Google Drive 처리 관련
MEMENTO_DRIVE_FOLDER_ID=optional_extra_folder_id
```

참고:
- `rag_chain.py`의 LLM 호출은 `llm.py:create_llm()`을 사용합니다.
- 임베딩(`OpenAIEmbeddings`)도 `llm.py:create_embeddings()`를 통해 프록시/가상키와 `LLM_EMBEDDING_MODEL`을 사용합니다.
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

### 처리

- `POST /process`  
  - `storage_type=local|drive|storage`
  - 로컬 디렉토리, Google Drive, Supabase Storage 파일 처리
- `POST /process/database`  
  - DB 레코드(`todolist`)를 문서로 변환해 벡터 저장
- `POST /process-output`  
  - 워크아이템 산출물 DOCX 생성 + Drive 업로드 + RAG 저장
- `GET /process/drive/status`  
  - Drive 폴더 인덱싱 백그라운드 작업 상태 조회

### 조회/질의

- `GET /retrieve`  
  - 유사도 검색 결과(원문 청크) 반환
- `GET /query`  
  - RAG 기반 최종 답변 + 소스 메타데이터 반환
- `POST /retrieve-by-indices`  
  - 선택한 `chunk_index` 목록으로 청크 직접 조회
- `GET /documents/list`  
  - 문서 목록 조회
- `GET /documents/chunks-metadata`  
  - 문서별 청크 메타데이터 조회

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

### 2) 검색

```bash
curl "http://localhost:8005/retrieve?query=교육&tenant_id=localhost&top_k=5"
```

### 3) 질의응답

```bash
curl "http://localhost:8005/query?query=프로젝트%20A의%20예산이%20얼마%20나왔지?&tenant_id=localhost"
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

## Supabase 스키마 참고

아래 테이블/함수가 필요합니다.

- `documents` (벡터 컬럼 포함)
- `document_images` (추출 이미지 메타)
- `processed_files` (중복 처리 방지)
- RPC 함수 `match_documents` (유사도 검색)

프로젝트에 맞는 정확한 DDL은 운영 중인 Supabase 스키마를 기준으로 관리하세요.

## 문제 해결

- `LLM_PROXY_API_KEY` 또는 `OPENAI_API_KEY`가 없으면 RAG LLM 초기화가 실패할 수 있습니다.
- `OPENAI_API_KEY`가 없으면 임베딩/일부 섹션 타이틀 생성이 실패할 수 있습니다.
- Drive 인증 오류 시 `/auth/google/url`로 OAuth URL을 먼저 발급하세요.
- 이미지 분석 실패 시 Supabase Storage 공개 URL 접근 가능 여부를 확인하세요.