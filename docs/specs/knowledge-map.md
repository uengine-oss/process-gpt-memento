# Spec: 지식지도

업로드된 문서가 어떤 단계를 거쳐 에이전트가 읽는 지도가 되는지, 각 단계의 성공 기준과
상태, 그리고 누가 어떤 API로 읽는지에 대한 계약. 근거는
[`../DESIGN_NOTES.md`](../DESIGN_NOTES.md#지도).

관련 코드: `app/api/knowledge_admin.py`, `app/services/ingest_queue.py`,
`app/services/document_processor.py`, `app/services/document_pages.py`,
`app/services/doc_cards.py`, `app/services/folder_cards.py`, `app/api/folders.py`,
`app/api/navigator.py`, `app/api/retrieve.py`

## 층

| 층 | 저장소 | 만드는 곳 | 읽는 곳 |
|---|---|---|---|
| 원문 페이지 | `document_pages` | `document_pages.save_pages` | codex 미러 `text/`, `/document/page`, `/document/grep` |
| 블록 | `document_blocks` | `document_blocks.save_blocks` | 카드·섹션 생성 |
| 섹션 | `document_sections` | `doc_sections.finalize` (카드와 같은 LLM 통과) | `/sections/search`, `/document/outline`, `/document/section` |
| 섹션 벡터 | 벡터 백엔드의 `kb_sections` 컬렉션 | `section_search.index_sections` | `/sections/search` |
| 문서 카드 | `knowledge_doc_cards` | `doc_cards.build_card` (백그라운드) | `CATALOG.tsv`, `/catalog`, `/folders/open` |
| 폴더 카드 | `knowledge_folder_cards` | `folder_cards.build_folder_card` (bottom-up) | `TREE.md`, `/folders/tree`, `/folders/open` |
| 검색 힌트 | 벡터 인덱스 + `documents` | `rag_chain.process_and_store_documents` | `/search`, `/documents/full-text` |

## 인제스트

1. `POST /knowledge/files/upload` 가 원본을 `files` 버킷에 두고 `index_status=pending` 으로 등록한다.
2. 인제스트 워커(`ingest_queue`)가 한 파일씩 `_index_uploaded_file` 을 돈다.
3. 형식별 파서가 페이지 단위 `Document` 목록을 만든다.
4. **페이지가 한 쪽이라도 저장되면 `indexed`** 다. 페이지가 있으면 에이전트가 읽을 수 있다.
   이어서 블록을 저장하고 `knowledge_files.parser_version` 을 기록한다. 블록 저장 실패는
   `indexed` 를 되돌리지 않는다.
5. 문서 카드는 페이지 저장 뒤 백그라운드로 만든다. 실패해도 `indexed` 를 되돌리지 않는다.
6. 벡터 인덱스는 그 뒤의 보조 단계다. 실패하면 `index_error` 에 `hints: ...` 로만 남는다.
7. 테넌트의 인덱싱이 멈추면(`pending`/`processing` 0) 바뀐 폴더와 조상의 폴더 카드를 다시 만든다.

### 페이지 단위

| 형식 | 한 "페이지" | 파서 |
|---|---|---|
| PDF | 실제 쪽 | `PDF_STRATEGY` (`pymupdf_region` 기본). 텍스트 없는 쪽·깨진 텍스트 레이어 쪽은 VLM OCR |
| PPTX | 슬라이드 | LibreOffice로 PDF 변환 후 PDF 파서 |
| XLSX | 시트 | openpyxl, 시트당 최대 20,000행 |
| DOCX, DOC | **문서 전체가 1쪽** | `docx_structured` (DOC는 LibreOffice로 DOCX 변환) |
| HWPX | **문서 전체가 1쪽** | `hwpx_structured` |
| HWP | **문서 전체가 1쪽** | rhwp 로 HWPX 변환 후 `hwpx_structured`, rhwp 가 없거나 실패하면 `vendor/extract_hwp`(글만) |
| TXT·MD·코드 | 문서 전체가 1쪽 | UTF-8 디코드 |

한글 문서는 확장자 대신 파일 첫 바이트로 형식을 가른다(HWP5 바이너리에 `.hwpx` 가 붙은 파일이 실제로 돈다).

PDF 본문은 PDF 에 기록된 글 순서를 따르고 표·그림 설명은 그 순서 안의 제자리에 끼운다. 여러 쪽의 위·아래
가장자리에 되풀이되는 글(머리말·꼬리말·쪽 번호)은 뺀다. 표는 병합 칸을 덮인 칸마다 같은 값으로 채운다(PDF·DOCX·HWPX 공통). 선이 없는 PDF 표는 글 블록 배치로 되살린다(`PDF_UNRULED_TABLES`, 기본 켬). 가로선 없이 한 칸에 묶인 PDF 표의 하위 행은 글자 줄로 나누고 이름 칸을 채운다(`PDF_SPLIT_SUBROWS`, 기본 켬). `MEMENTO_TABLE_LLM` 을 켜면 PDF 표를 LLM 으로 다시 읽고, 숫자가 모두 PDF 글자에 있을 때만 그 표를 쓴다(기본 끔, [configuration.md](configuration.md#pdf-표-llm-파싱-memento_table_llm)).
그림은 짧은 변 80px 미만을 설명하지 않고, 같은 그림은 한 번만 설명해 처음 나온 자리에만 넣는다.

DOCX·HWPX·HWP는 흐르는 문서라 파일에 쪽 정보가 없다. 렌더러로 계산한 쪽은 사용자가
보는 쪽과 다르므로 쪽 번호로 인용하지 않는다([근거](../DESIGN_NOTES.md#흐르는-문서의-쪽-번호)).

### 블록

블록은 인용 앵커다. `document_blocks(tenant_id, file_id, block_index, kind, text,
heading_level, page_number, bbox)`.

- `kind`: `paragraph` | `table` | `image`(그림 설명).
- DOCX·HWPX는 파서의 구조를 그대로 쓴다. `page_number` 는 `null` 이다.
  `heading_level` 은 파일에 명시된 헤딩만 — DOCX 제목 스타일·`outlineLvl`(basedOn 상속),
  HWPX 개요 수준 문단 모양. 번호 패턴으로 추측하지 않는다. DOCX 콘텐츠 컨트롤(`sdt`) 안도 읽는다.
- 그 밖의 형식은 쪽 본문을 빈 줄로 나누고 쪽 번호를 단다. PDF는 `blocks_json` offset으로 `bbox` 를 붙인다.
  HWP(`extract_hwp`)·TXT는 쪽이 없어 `page_number` 가 `null` 이다.
- 2,000자를 넘는 블록은 줄 경계에서 나눈다. 표는 행 경계에서 나누고 머리행을 반복한다. 헤딩은 나누지 않는다.
- 파서 출력이 바뀌면 `app/plugins/parsers.PARSER_VERSION` 을 올린다.

## 상태

`knowledge_files.index_status`: `pending` → `processing` → `indexed` | `failed` (`excluded` 는 인제스트 대상 아님).
실패의 최종 판정은 큐가 한다. 일시 오류는 재시도하고, 영구 오류만 `failed` 로 굳힌다.

문서 카드 `status`: `pending` | `done` | `failed` | `empty`(텍스트 없음).

지도의 문서 상태(`folders._doc_state`): `ready` | `pending` | `failed` | `no_text`.
`no_text` 는 스캔본처럼 읽을 수 없는 문서다. "자료에 없음"과 다른 결론이다.
`/folders/tree` 가 폴더별로 집계하고, 관리 화면과 에이전트가 같은 숫자를 본다.

## 카드

**문서 카드**는 "이 문서를 열어야 하는가"를 답한다.

- 블록을 12,000자 창(`doc_cards.WINDOW_CHARS`)으로 묶어 순서대로 읽으며 갱신한다. 창은 블록 경계에서
  자르고 블록마다 `[b12]` 앵커와 명시적 헤딩 표시 `[H1]` 을 붙인다. 블록이 없는 옛 문서는 페이지에서
  블록을 만들어 저장한 뒤 읽는다.
  창이 16개(`doc_cards.MAX_WINDOWS`)를 넘으면 문서 전체에 고르게 골라 읽고 `coverage` 에 남긴다.
- 서명은 `v{CARD_VERSION}:{모델}:{본문 해시}`. 같은 내용의 문서는 같은 버전의 카드만 재사용하고 섹션도 복사한다.
- 같은 폴더의 다른 문서 제목(최대 12)을 함께 보여 준다. summary 첫 문장과 `distinguishers` 는
  옆 문서와 구별되는 사실(사업명·발주처·상대방·연도·차수·버전)이다.
- 필드: `title` `summary` `doc_type` `distinguishers` `topics` `entities` `keywords`
  `language` `answers_questions` `coverage`.
- 같은 내용(`content_sha256`)이면 카드를 재사용한다.

**섹션**은 블록 위의 목차다. `document_sections(tenant_id, file_id, section_index, start_block,
end_block, title, summary, chars, source)`.

- 카드와 같은 창 호출이 "이 창에서 새 섹션이 시작되는 블록"과 제목·한 문장 요약을 함께 돌려준다.
  창 밖 블록을 가리키는 답은 버린다. `[H1]` 은 판단 근거일 뿐 그대로 섹션이 되지 않는다.
- 섹션은 문서 전체를 빈틈없이 덮는다. 첫 섹션이 block 0 이 아니면 `(앞부분)` 을 넣고, 같은 제목이
  연달아 나오면 하나로 친다.
- `doc_sections.SECTION_MAX_CHARS`(8,000자)를 넘는 섹션은 LLM 으로 한 번 더 나누고(`상위 › 하위`),
  못 나누면 블록 경계에서 크기로 자른다(`source=split`, 제목 `(계속 n: 첫 내용)`).

**폴더 카드**는 자식 문서 카드와 하위 폴더 카드를 bottom-up 으로 모아 폴더당 LLM 1회로 만든다.
필드: `summary` `topics` `reading_guide` `start_with` `answers_questions` + 결정론 필드
(문서 수·기간·종류·후보 엔티티). `start_with` 는 실제 파일명만 남긴다.

## 지도 API

| API | 용도 |
|---|---|
| `GET /folders/tree` | 폴더 골격 + 폴더 카드 + 준비 상태 |
| `GET /folders/open` | 한 폴더의 하위 폴더 + 문서 카드. `include_refs=true` 면 관리 필드 포함 |
| `GET /folders/card` | 폴더 카드 1건 |
| `GET /catalog` | 선택 범위의 문서 카드 목록 |
| `GET /document/grep` · `/document/page` · `/document/raw` | 본문 검색 · 페이지 읽기 · 원본 바이트 |
| `GET /search` | 벡터 top-k. `file_ids` / `folder_paths` 로 범위 제한 |

### 섹션 API

| API | 용도 |
|---|---|
| `GET /sections/search` | 섹션 순위. `query` + `file_ids`/`folder_paths`(둘 다 비면 테넌트 전체), `top_k` ≤ 100 |
| `GET /document/outline` | 문서의 섹션 목차. `file_id` 또는 `path`. 섹션이 없으면 `ready=false` |
| `GET /documents/outlines` | 선택 범위(`file_ids`/`folder_paths`) 전체의 목차를 `{file_id: [섹션]}` 으로. 섹션 없는 파일은 빠진다 |
| `GET /document/section` | 섹션(`section_index`) 또는 블록 범위(`start_block`~`end_block`) 본문. 블록마다 `[bN]` 앵커, 20,000자에서 자르고 `truncated` 로 알린다 |

### 섹션 검색

- 키워드: `kb_section_keyword_search` RPC. 질문을 2자 이상 토큰으로 나누고 끝의 한국어 조사를 뗀 형태도
  넣는다(최대 12개). 블록 본문 부분 일치(pg_trgm)를 섹션으로 모아 용어별 tf·df 로 BM25 모양 점수를 매기고,
  섹션 제목·요약에 걸리면 한 번 더 센다.
- 벡터: 섹션마다 `파일명 › 제목\n요약\n본문` 앞 4,000자(`section_search.EMBED_CHARS`)를 임베딩해
  `kb_sections` 컬렉션에 둔다. 청크 힌트 컬렉션과 섞지 않는다.
- 두 순위를 RRF(k=60)로 합친다. 한쪽이 실패하면 다른 쪽 결과만으로 답하고 `errors` 에 남긴다.
- 결과 항목: `file_id` `file_name` `path` `section_index` `title` `summary` `start_block` `end_block`
  `chars` `pages`(쪽 있는 문서만 `[처음, 끝]`) `score` `ranks` `matched_terms` `snippet`.
- 파일 삭제·재인덱싱은 블록·섹션·섹션 벡터를 함께 지운다(`section_search.forget_files`).

선택 범위는 `file_ids` 또는 `folder_paths` 로 받는다. 폴더째 선택을 수천 개 `file_id` 로
풀어 보내지 않는다.

### 인용 뷰어 API

인용 앵커는 `file_id` + `start_block`~`end_block` 이다. 쪽·bbox 는 그 블록에 붙은 속성이다.

| API | 용도 |
|---|---|
| `GET /document/blocks` | 문서 전체 블록과 섹션. 블록마다 칠할 자리 `rects`(`[{page, bbox}]`). PDF는 원본 쪽(`page_basis=original`), 흐르는 문서는 변환본 쪽(`page_basis=rendition`). 렌더러가 없거나 `render=false` 면 `layout=flowing`, `rects` 없음 |
| `GET /document/page-image` | PDF(흐르는 문서는 변환본) 한 쪽(`page`, 1부터)을 PNG로. `scale` 기본 1.5. bbox 는 PDF 포인트 단위라 이미지 픽셀 ÷ `scale` 로 맞춘다 |
| `GET /document/locate` | 인용 문장(`quote`)이 걸친 블록 범위. `...`·`…` 은 사이 400자 이내 생략으로 본다. 글자·숫자만 비교하고(태그는 먼저 지움), 정확 일치가 없으면 낱말 사이 40자 끼어듦을 허용해 `loose: true` 로 돌려준다. 같은 문장이 여러 곳이면 모두 `matches` 로(항목마다 `section`) |

변환본(`app/services/rendition.py`): HWPX·HWP 는 rhwp(`HWPX_RHWP` → PATH → codex 런타임 경로), DOCX·DOC·RTF·ODT 는
LibreOffice 로 PDF를 만들고 `RENDITION_CACHE_DIR`(기본 `.cache/renditions`)에 원본 해시로 둔다. 블록은 변환본 글자 흐름
(글자·숫자만)에 맞춘다 — 한 번만 나오는 블록 중 순서가 맞는 것을 고정점으로 잡고, 나머지는 고정점 사이에서만 찾는다.
인제스트가 블록을 저장한 직후 변환을 백그라운드로 미리 돌린다(`RENDITION_PREWARM`, 기본 켬, 한 번에
하나씩). 변환본 PDF와 블록 배치는 로컬 캐시와 함께 비공개 버킷
`artifacts/renditions/` 에도 둬서 파드가 바뀌어도 다시 그리지 않는다. 배치 키에는 블록 내용 해시가 들어가
재인덱싱으로 블록이 바뀌면 배치만 다시 한다. 미리 변환이 실패했으면 첫 열람 때 변환한다(HWPX 수십 초). 미리 변환이 생기기 전에 올라온 문서는 `python -m scripts.prewarm_renditions <tenant> [--folder]` 로 채운다(있으면 건너뜀).
변환본 쪽 번호는 보기용이며 인용 앵커가 아니다. rhwp 는 다단을 그리지 않고 쪽을 넘기는 표를 잘라, HWPX 변환본의 쪽 모양은 원본과 다를 수 있다. 컨테이너에는 rhwp 와 한글 글꼴(fonts-nanum, fonts-noto-cjk)이 있어야 한다(Dockerfile).

## 호출처

엔드포인트를 지우거나 응답 모양을 바꾸기 전에 여기서 호출처를 확인한다. 대부분 실패하면
빈 결과로 조용히 넘어가서, 깨져도 에러가 나지 않는다.

| 호출처 | 엔드포인트 |
|---|---|
| codex | `/catalog` `/folders/tree` `/folders/open` `/document/grep` `/document/page` `/document/raw` `/documents/full-text` `/search` `/glossary/terms` `/summarize` `/process-session-file` `/sections/search` `/documents/outlines` `/document/outline` `/document/section` `/document/locate`(인용 게이트), RPC `kb_page_text` |
| vue3 | `/knowledge/*`, `/folders/card`, `/documents/list`, `/artifact-url`, `/save-to-storage`, `/save-to-drive`, `/process`, `/process/drive/status`, `/parse/stored`, `/auth/google/*`, `/document/blocks` `/document/page-image` `/document/locate`(인용 뷰어) |
| agent-sdk | `/retrieve` |
| office-mcp | `/documents/chunks-metadata` `/retrieve-by-indices` `/preview/pdf-highlight` |

## 스키마 전제

지도 API는 `sql/knowledge_doc_cards.sql`, `sql/knowledge_folder_cards.sql`, `sql/kb_mirror.sql`
을 전제로 한다. 블록 저장은 `sql/document_blocks.sql`, 섹션 저장은 `sql/document_sections.sql` 을 전제로 한다
(없으면 경고만 남기고 인제스트·카드는 계속). `/folders/tree`·`/folders/open` 이 `knowledge_files.has_text`/`page_count` 를
읽고, codex 미러가 `kb_page_text` RPC로 전문을 배치로 가져간다.
