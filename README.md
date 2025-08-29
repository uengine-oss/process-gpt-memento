# Process GPT Memento

AI 기반 문서 처리 및 질의응답 시스템

## 주요 기능

### 📄 문서 처리
- **PDF, DOCX, PPTX, TXT** 등 다양한 문서 형식 지원
- **Google Drive** 및 **Supabase Storage** 연동
- **텍스트 추출** 및 **청킹** 처리

### 🖼️ 이미지 처리 (신규!)
- **문서 내 이미지 자동 추출**
- **OpenAI Vision API**를 사용한 이미지 내용 분석
- **이미지 설명을 텍스트로 변환**하여 벡터 저장
- 지원 형식: JPG, PNG, GIF

### 🔍 벡터 검색
- **Supabase Vector Store** 기반 임베딩 저장
- **이미지 내용 포함** 통합 검색
- **유사도 기반** 문서 검색

### 🔐 멀티 테넌트 지원
- **OAuth 2.0** 기반 Google Drive 인증
- **테넌트별** 문서 및 이미지 관리
- **격리된** 데이터 접근

## 설치 및 설정

### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 환경 변수 설정
`.env` 파일을 생성하고 다음 변수들을 설정하세요:

```env
# Supabase 설정
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key

# OpenAI 설정
OPENAI_API_KEY=your_openai_api_key
OPENAI_API_BASE=https://api.openai.com/v1  # 선택사항

# Google Drive OAuth 설정
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
```

### 3. 데이터베이스 설정
Supabase에서 다음 테이블들이 필요합니다:

```sql
-- 문서 테이블
CREATE TABLE documents (
    id UUID PRIMARY KEY,
    content TEXT,
    metadata JSONB,
    embedding vector(1536)
);

-- 이미지 메타데이터 테이블
CREATE TABLE document_images (
    id UUID PRIMARY KEY,
    document_id UUID REFERENCES documents(id),
    tenant_id TEXT,
    image_id TEXT,
    image_url TEXT,
    download_url TEXT,
    metadata JSONB
);

-- 처리된 파일 추적 테이블
CREATE TABLE processed_files (
    id UUID PRIMARY KEY,
    file_id TEXT,
    tenant_id TEXT,
    file_name TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## 사용법

### 1. 서버 실행
```bash
python main.py
```

### 2. Google Drive 문서 처리
```bash
curl -X POST "http://localhost:8000/process/drive" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "your_tenant_id",
    "file_path": "google_drive_file_id"
  }'
```

### 3. 이미지 추출 테스트
```bash
python test_image_extraction.py
```

## 이미지 처리 워크플로우

1. **문서 업로드**: PDF, DOCX, PPTX 파일을 Google Drive에 업로드
2. **이미지 추출**: 문서 내 이미지를 자동으로 감지하고 추출
3. **Google Drive 저장**: 추출된 이미지를 Google Drive에 저장
4. **AI 분석**: OpenAI Vision API로 이미지 내용을 텍스트로 변환
5. **벡터 저장**: 텍스트 + 이미지 설명을 통합하여 벡터 저장소에 저장
6. **통합 검색**: 텍스트와 이미지 내용을 모두 포함한 검색 가능

## 지원 파일 형식

| 형식 | 텍스트 추출 | 이미지 추출 | 이미지 형식 |
|------|-------------|-------------|-------------|
| PDF | ✅ | ✅ | JPG, PNG, GIF |
| DOCX | ✅ | ✅ | JPG, PNG, GIF |
| PPTX | ✅ | ✅ | JPG, PNG, GIF |
| TXT | ✅ | ❌ | - |

## 문제 해결

### 이미지 추출이 안되는 경우
1. **OpenAI API 키** 확인
2. **문서 형식** 지원 여부 확인
3. **로그** 확인하여 오류 메시지 파악

### 벡터 저장 실패
1. **Supabase 연결** 상태 확인
2. **데이터베이스 테이블** 존재 여부 확인
3. **환경 변수** 설정 확인

## 라이선스

MIT License

## 기여

버그 리포트 및 기능 제안은 이슈로 등록해 주세요.