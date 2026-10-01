# Repository instructions

이 저장소에서 작업할 때 지켜야 할 규칙. **왜** 만드는지는 [`INTENT.md`](INTENT.md),
**무엇을** 만들어야 하는지는 [`docs/specs/`](docs/specs/)에 있다.
계약을 바꾸는 변경이라면 코드보다 해당 spec을 먼저 읽는다.

## 스택

- Python 3.11+, FastAPI + Uvicorn, Pydantic
- Supabase(PostgREST/Storage) — 파일 목록, 페이지, 카드, 원본 파일
- 벡터 인덱스 — `VECTOR_BACKEND` (`chroma` 기본, `qdrant` 지원). 검색 힌트 전용
- LLM·임베딩 — `app/services/llm.py`, 프로바이더는 `MEMENTO_LLM_PROVIDER`
- LibreOffice(`soffice`) — DOC·PPTX 변환과 HWP 폴백

## 명령

```bash
.venv/Scripts/python.exe -m pytest -q      # 전체 테스트 (Windows)
.venv/bin/python -m pytest -q              # 전체 테스트 (POSIX)
python main.py                             # 서버 실행 (:8005)
```

## 코드 배치

```
app/api/           라우터. 지도(folders, navigator), 관리(knowledge_admin), 조회(retrieve), 인제스트(ingest)
app/services/      인제스트 큐, 페이지 저장, 문서·폴더 카드, 벡터 인덱스, LLM
app/plugins/       형식별 파서(parsers), 청커(chunkers)
app/storage/       스토리지 로더, 산출물 버킷
app/core/config.py 설정 목록과 기본값
sql/               Supabase 마이그레이션 (배포 전에 적용)
scripts/           카드 백필 등 운영 CLI
vendor/extract_hwp HWP5·HWPX 추출 (벤더링)
```

## 규칙

### 주석
국소적인 "왜"만 한 줄로 적는다. 계약이면 `docs/specs/`, 결정의 근거면
[`docs/DESIGN_NOTES.md`](docs/DESIGN_NOTES.md)에 적고 주석에는 앵커만 남긴다.

### 품질은 원문과 대조한다
파서·카드·검색 결과가 맞는지는 **원본 파일과 직접 대조**해서 판단한다. 파서 출력이나
모델 출력만 보고 "정상"이라 결론 내리지 않는다. HWPX 파서가 탭 뒤 본문을 버려 한 문서의
85%가 사라졌는데, 출력만 봐서는 헤딩이 멀쩡해 보였다.

### 파서를 바꾸면 재인덱싱을 계획한다
이미 저장된 페이지·카드는 옛 파서의 결과다. 코드만 고치면 DB는 그대로 틀려 있다.

### 엔드포인트를 지우기 전에 호출처를 찾는다
codex·vue3·agent-sdk·office-mcp가 이 서비스를 부른다. 호출처 대부분은 실패하면 조용히 빈
결과로 넘어가서, 지워도 에러가 나지 않고 기능만 사라진다. 호출처 목록은
[`docs/specs/knowledge-map.md`](docs/specs/knowledge-map.md#호출처)에 있다.

### 설정은 서버가 정하고 한 표에 모은다
설정은 배포마다 환경 변수로 정한다. 저장되는 결과(파싱·색인)를 바꾸는 값을 호출처의 요청 파라미터로 받지 않는다.
목록·기본값·뜻은 [`docs/specs/configuration.md`](docs/specs/configuration.md), 읽는 코드는 주로 `app/core/config.py`.
새 설정은 `MEMENTO_` 로 시작하고 configuration.md 와 `.env.example` 을 같이 고친다. LLM 샘플링은 `config/llm_sampling.json`.

### 계약을 바꾸면 spec을 같이 고친다
`docs/specs/` 아래 문서는 현재 코드의 계약이다. 코드만 바꾸고 두면 다음 사람이 spec을 믿고 틀린다.

## 배포

- **`main` push가 곧 배포다.** GitHub Actions가 이미지를 GHCR에 올리고
  `process-gpt-k8s`의 `deployments/memento-deployment.yaml` 이미지 태그를 바꾼다.
  release 생성은 운영 배포(`deploy-prod.yaml`).
- `sql/` 마이그레이션은 자동으로 돌지 않는다. 스키마를 바꾸는 변경은 배포 환경 Supabase에
  **먼저** 적용하고 push한다.

## 보안

- `.env`, API 키, Supabase 자격증명, Google 자격증명을 커밋하지 않는다.
- `files` 버킷은 공개다. 에이전트 산출물은 비공개 버킷에만 둔다
  ([`docs/specs/artifact-bucket.md`](docs/specs/artifact-bucket.md)).
