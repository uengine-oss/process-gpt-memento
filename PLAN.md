# Plan: 블록·섹션 지식베이스 (`kb-blocks` 브랜치)

이 브랜치의 작업 계획. 구현이 계획에서 벗어나면 같은 커밋에서 이 파일을 고친다.
머지할 때 계약은 `docs/specs/`, 근거는 `docs/DESIGN_NOTES.md` 로 옮기고 이 파일은 지운다.

## 목표

흐르는 문서(DOCX·HWPX)가 "문서 전체 = 1쪽"으로 저장돼 위치 인용·검색·카드가 문서 단위로
뭉개지는 문제를 없앤다. 근거: [DESIGN_NOTES — 흐르는 문서의 쪽 번호](docs/DESIGN_NOTES.md#흐르는-문서의-쪽-번호),
[섹션](docs/DESIGN_NOTES.md#섹션).

- **블록** = 인용 앵커. 문단·표(행 묶음)·그림 설명. 모든 형식에 항상 있다.
- **섹션** = 탐색 단위. 명시적 헤딩이 있으면 쓰고, 없으면 카드 LLM이 고른다.
- 쪽 번호는 PDF·PPTX·XLSX(시트)만. 흐르는 문서는 `null`.

## 단계

### 1. 블록 저장 + 파서 버전 ✅
- `sql/document_blocks.sql`: `document_blocks(tenant_id, file_id, block_index, kind, text,
  heading_level, page_number, bbox)`, `knowledge_files.parser_version`.
- 파서가 블록을 `Document.metadata["_blocks"]` 로 내보낸다(청크 메타데이터에서는 걸러낸다).
  DOCX(`docx_structured` 블록 + 스타일 헤딩), HWPX(토큰 + 개요 수준 헤딩),
  PDF(`blocks_json` offset/bbox). 나머지는 빈 줄·크기 경계로 나눈다.
- `document_pages` 는 그대로 둔다(codex 미러가 읽는다). 블록 저장 실패는 인제스트를 막지 않는다.
- 완료 조건: 형식별 단위 테스트, 로컬 코퍼스 재인덱싱 후 원문 대비 블록 텍스트 보존율 ≥ 99%.
- 결과(2026-09-23, 로컬 고유 파일 295건, 비전 끔): 형식별 99.0~100.8%, 99% 미만 2건(98.7%, 98.9%).
  실제 인제스트 경로로 6건(HWPX·DOCX·PDF·XLSX·PPTX) 저장 확인. 전체 재인덱싱은 아직.

### 2. 섹션 분할 검증 ✅
- 성격이 다른 문서 10~20건(회의록·공문·매뉴얼·표 위주)으로 명시적 헤딩 + LLM 분할 결과를 뽑아 본다.
- 완료 조건: 사람이 보고 목차로 쓸 만하다고 판단.
- 1차 결과(2026-09-23, 15건: 식약처 회의록·서울시 추진계획·입법예고·GOV.UK 정책 + 로컬 코퍼스,
  보고서 `test-field/paged_parse_test/section_samples/REPORT.md`):
  - LLM 분할은 문서 종류에 맞게 나뉜다 — 회의록은 안건, 계획서는 Ⅰ~Ⅴ·□, 계약은 조항, RFI는 번호.
    문서당 1~3회 호출. OpenAI 3~33초, frentis 1.5~5초.
  - frentis 는 더 굵고 흔들린다: RFI 1.1~1.4 를 11,828자 한 섹션으로 묶고 2.4.1 을 건너뜀, 같은 제목 중복.
  - 명시적 헤딩을 그대로 믿으면 안 된다: GOV.UK DOCX 는 본문 문단에 헤딩 스타일이 붙어 44섹션 중 상당수가 본문.
    → 헤딩은 LLM 입력에 표시만 하고 판단은 LLM 이 한다.
  - 크기 상한이 필요하다: 표 하나짜리 붙임이 48,842자 한 섹션. 상한을 넘으면 그 섹션만 한 번 더 나눈다.

### 3. 섹션 + 카드를 한 번의 LLM 통과로 ✅
- 카드 창 호출이 섹션 시작·제목·요약을 함께 돌려준다. `document_sections` 저장.
- 문서 카드는 섹션 카드의 롤업. 카드·섹션 백필.
- 결과(2026-09-23): 창을 블록 경계로 자르고 `[b12]`·`[H1]` 을 붙임. 8,000자 상한 → LLM 재분할, 실패 시 크기 분할.
  인제스트·카드 재생성 API·백필 스크립트가 같은 `build_and_store_card` 를 탄다. CARD_VERSION 4.
  받은 샘플 6건을 로컬 KB `section_samples/` 에 올려 frentis·OpenAI 로 끝까지 확인. 헤딩 표시가 붙은 본문
  (GOV.UK)은 섹션이 되지 않았고, 48,842자 붙임은 6,541자 이하로 나뉨. 식약처 203차 [제8호]는 두 모델 다
  [제7호]에 묶음(섹션 누락 1건). 로컬 KB 전체 백필은 아직.

### 4. 섹션 단위 인덱스와 API ✅
- 섹션 본문 + 섹션 카드를 키워드·벡터로 색인. `search` 가 섹션·블록 범위를 돌려준다.
- `read(file, section | 블록 범위)`, 문서별 OUTLINE.
- 결과(2026-09-23): `/sections/search`(키워드 RPC + `kb_sections` 벡터, RRF), `/document/outline`,
  `/document/section`. 샘플 4개 질의 모두 정답 섹션 1위(키워드·벡터 모두 1위가 대부분).
  카드 생성 뒤 섹션 벡터를 색인하고, 삭제·재인덱싱 때 블록·섹션·벡터를 함께 지운다.

### 5. codex 연결과 벤치마크 ✅ (1차, 이후 Jev 는 LLM logprobs 판정으로 대체 — codex DESIGN_NOTES#kb-router)
- 미러에 OUTLINE, 서버 후보 생성, Jev 재순위·인용 검증, 인용 = 섹션 경로 + 발췌.
- Jev on/off 시간·토큰·정확도 비교.
- 결과(2026-09-23): codex `kb-sections` 브랜치 — 턴 준비 때 `CANDIDATES.md`(섹션 검색 → Jev 재정렬),
  `outline/`, `SCOPE.json`. MCP `knowledge_search`·`knowledge_read` 가 섹션을 읽고, Jev 는 순서만 바꾼다.
  `docs` 64건 카드·섹션 백필(471초). 검색만으로 정답 파일@15 97%, 정답 쪽@15 93%(그림 문항 쪽@15 79%).
  20문항 짝 비교: Jev on 이 시간 중앙값 21.6초 대 26.9초, 에이전트 토큰 64,783 대 100,934, 대신 Jev 토큰
  문항당 약 23,000. 정확도 85% 대 95%(짝: on 이 나음 1·나쁨 3). 표본이 작아 결론 보류.
  원자료 `test-field/kb-map-testset/jev-2026-09-23/`. 인용 검증(Jev)은 아직 안 함.

### 6. 출처 뷰어 ✅ (1차, 2026-09-28~29)
- 답변 인용 `[[경로 › 섹션 · bS-bE · "발췌"]]` → 칩 → 원문처럼 보이는 화면에 발췌 자리를 칠한다. HWPX·DOCX 는 변환본 PDF
  (rhwp·LibreOffice, 인제스트 직후 미리 변환, 비공개 버킷에 보관) 위에 칠한다. API `/document/blocks`·`page-image`·`locate`
  (계약 `docs/specs/knowledge-map.md#인용-뷰어-api`), codex `anchor_quotes`, vue3 `CitationViewer.vue`.
- 앵커는 발췌이고 블록 번호는 같은 문장이 여러 곳일 때의 힌트. locate 는 글자·숫자만 비교, 정확히 없을 때만 낱말 사이
  40자 끼어듦 허용. 서버는 못 찾은 인용을 `anchored: false` 로 표시만 한다(막지 않는다).
- 결과(떼어 둔 검증셋 Allganize 32 + KoPub 24): 인용 249개 전부 서버 확인, 정답 쪽 칠함 44~45/56, 정답 채점 0.74~0.81
  (채점 모델에 따라). 근거·측정은 codex `docs/DESIGN_NOTES.md#citation-anchor`, 이 저장소 DESIGN_NOTES "흐르는 문서의 쪽 번호".
  원자료 `test-field/출처테스트/citation-bench-2026-09-29/`.
- 로컬에서 knowledge MCP 를 바꾸면 codex 대화 컨테이너 이미지를 다시 빌드해야 한다(codex AGENTS.md). 안 하면 옛 도구로 잰다.

### 7. 파서 점검·수정 ✅ (2026-09-30, 미커밋·미배포)
- 벤치 `test-field/parser-bench/`: 합성(명세 하나 → DOCX·HWPX·PDF), 실제 공공 HWPX·PDF 짝 18건, olmOCR-Bench 부분셋,
  KoPub·Allganize PDF. 결함·조치·전후 수치는 DESIGN_NOTES "파서 점검", 읽기 순서 결정 근거는 "PDF 읽기 순서".
- 고친 것: PDF 기록 순서·표에 걸친 글 보존·가짜 표 버림·안 그려진 그림 건너뜀·깨진 텍스트 레이어 OCR·반복 머리말 제거·
  쪽 넘김 표 머리행, 기본 전략 `pymupdf_region`, DOCX 병합 격자, HWPX·DOCX 병합 값 채우기·그림 크기/중복 필터,
  HWPX 머리말 제외, HWP → rhwp HWPX 변환, 한글 문서 형식 판별, 괘선 없는 표 되살림,
  선 없는 하위 행 나누기(10-01, DESIGN_NOTES "선 없는 하위 행"). `PARSER_VERSION = 2026-10-01.subrows`.
- 표 LLM 파싱 옵션 `MEMENTO_TABLE_LLM`(기본 끔, 10-01, DESIGN_NOTES "PDF 표: 규칙 대 LLM"). 설정 전체는
  `docs/specs/configuration.md`, `.env.example` 과 맞춘다. 켜는 배포는 `reindex_stale --ext pdf`.
- 재인덱싱: `python -m scripts.reindex_stale <tenant> [--folder] [--ext] [--dry-run]` 이 옛 버전 파일을 pending 으로
  돌리고 서버 sweeper 가 다시 인덱싱한다. 그림 설명·카드 LLM 을 다시 부르므로 형식·폴더를 나눠 돌린다
  (먼저 `--ext hwp,pdf`: 개선 폭이 크다). 로컬 dry-run 622건.

## 남은 일 (다음에 이어서)

1. **파서 남은 것** — 표 영역이 이름 열을 빼고 잡히는 PDF 표, 벡터 차트, 제호를 본문 뒤에 기록한 PDF,
   XLSX 병합 셀, 암호화 HWPX, 한글로 깨진 텍스트 레이어, 장식 그림 가리기. 폐쇄망 VLM 으로 그림 사실 회수 재측정.
2. **배포** — memento·codex·vue3 모두 미push(`main` push = 배포). 배포 뒤 `scripts.reindex_stale` 로 재인덱싱. memento 이미지에 rhwp·한글 폰트(Dockerfile). 배포 뒤
   `python -m scripts.prewarm_renditions <tenant>` 로 기존 문서 변환본을 채운다(HWPX 첫 열람 30~70초 방지).
3. **검색 품질** — 일반 문서에서 정답 파일을 인용 못 한 문항이 56개 중 6~7개. 인용이 아니라 문서 찾기 문제.
4. **생각 과정 유출** — 최종 답에 모델의 생각이 섞이는 일(웹 인용 표식이 있을 때만 잡는다). 일반 탐지는 미해결.
5. 검증하지 않은 상수: 발췌 여럿 사이 구분자 8자, 말줄임 사이 400자. 표본으로만 확인한 상수: 머리말 띠 쪽 높이 10%·쪽 절반 반복, 깨진 글 판별 50%·80%.
6. 로컬 KB 전체 재인덱싱(블록·섹션·섹션 벡터), 배포 환경 SQL 선적용(1~4단계 몫).
7. 머지 전: 계약은 specs 에 있다. 근거를 DESIGN_NOTES 로 옮기고 이 파일을 지운다.

## 위험

- 기존 515건 재인덱싱 필요(페이지·카드·벡터 전부).
- 배포 환경에 SQL 선적용 필요(`main` push = 배포).
- codex 가 `document_pages` 형식에 묶여 있다 — 4단계 전까지 형식을 바꾸지 않는다.
