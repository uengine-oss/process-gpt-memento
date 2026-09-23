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
| 문서 카드 | `knowledge_doc_cards` | `doc_cards.build_card` (백그라운드) | `CATALOG.tsv`, `/catalog`, `/folders/open` |
| 폴더 카드 | `knowledge_folder_cards` | `folder_cards.build_folder_card` (bottom-up) | `TREE.md`, `/folders/tree`, `/folders/open` |
| 검색 힌트 | 벡터 인덱스 + `documents` | `rag_chain.process_and_store_documents` | `/search`, `/documents/full-text` |

## 인제스트

1. `POST /knowledge/files/upload` 가 원본을 `files` 버킷에 두고 `index_status=pending` 으로 등록한다.
2. 인제스트 워커(`ingest_queue`)가 한 파일씩 `_index_uploaded_file` 을 돈다.
3. 형식별 파서가 페이지 단위 `Document` 목록을 만든다.
4. **페이지가 한 쪽이라도 저장되면 `indexed`** 다. 페이지가 있으면 에이전트가 읽을 수 있다.
5. 문서 카드는 페이지 저장 뒤 백그라운드로 만든다. 실패해도 `indexed` 를 되돌리지 않는다.
6. 벡터 인덱스는 그 뒤의 보조 단계다. 실패하면 `index_error` 에 `hints: ...` 로만 남는다.
7. 테넌트의 인덱싱이 멈추면(`pending`/`processing` 0) 바뀐 폴더와 조상의 폴더 카드를 다시 만든다.

### 페이지 단위

| 형식 | 한 "페이지" | 파서 |
|---|---|---|
| PDF | 실제 쪽 | `PDF_STRATEGY` (`pymupdf` 기본). 텍스트 없는 쪽은 VLM OCR |
| PPTX | 슬라이드 | LibreOffice로 PDF 변환 후 PDF 파서 |
| XLSX | 시트 | openpyxl, 시트당 최대 20,000행 |
| DOCX, DOC | **문서 전체가 1쪽** | `docx_structured` (DOC는 LibreOffice로 DOCX 변환) |
| HWPX | **문서 전체가 1쪽** | `hwpx_structured` |
| HWP | **문서 전체가 1쪽** | `vendor/extract_hwp`, 실패 시 PDF 변환 폴백 |
| TXT·MD·코드 | 문서 전체가 1쪽 | UTF-8 디코드 |

DOCX·HWPX·HWP는 흐르는 문서라 파일에 쪽 정보가 없다. 렌더러로 계산한 쪽은 사용자가
보는 쪽과 다르므로 쪽 번호로 인용하지 않는다([근거](../DESIGN_NOTES.md#흐르는-문서의-쪽-번호)).

## 상태

`knowledge_files.index_status`: `pending` → `processing` → `indexed` | `failed` (`excluded` 는 인제스트 대상 아님).
실패의 최종 판정은 큐가 한다. 일시 오류는 재시도하고, 영구 오류만 `failed` 로 굳힌다.

문서 카드 `status`: `pending` | `done` | `failed` | `empty`(텍스트 없음).

지도의 문서 상태(`folders._doc_state`): `ready` | `pending` | `failed` | `no_text`.
`no_text` 는 스캔본처럼 읽을 수 없는 문서다. "자료에 없음"과 다른 결론이다.
`/folders/tree` 가 폴더별로 집계하고, 관리 화면과 에이전트가 같은 숫자를 본다.

## 카드

**문서 카드**는 "이 문서를 열어야 하는가"를 답한다.

- 전문을 12,000자 창(`KB_CARD_WINDOW_CHARS`)으로 나눠 순서대로 읽으며 갱신한다.
  창이 16개(`KB_CARD_MAX_WINDOWS`)를 넘으면 문서 전체에 고르게 골라 읽고 `coverage` 에 남긴다.
- 같은 폴더의 다른 문서 제목(최대 12)을 함께 보여 준다. summary 첫 문장과 `distinguishers` 는
  옆 문서와 구별되는 사실(사업명·발주처·상대방·연도·차수·버전)이다.
- 필드: `title` `summary` `doc_type` `distinguishers` `topics` `entities` `keywords`
  `language` `answers_questions` `coverage`.
- 같은 내용(`content_sha256`)이면 카드를 재사용한다.

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

선택 범위는 `file_ids` 또는 `folder_paths` 로 받는다. 폴더째 선택을 수천 개 `file_id` 로
풀어 보내지 않는다.

## 호출처

엔드포인트를 지우거나 응답 모양을 바꾸기 전에 여기서 호출처를 확인한다. 대부분 실패하면
빈 결과로 조용히 넘어가서, 깨져도 에러가 나지 않는다.

| 호출처 | 엔드포인트 |
|---|---|
| codex | `/catalog` `/folders/tree` `/folders/open` `/document/grep` `/document/page` `/document/raw` `/documents/full-text` `/search` `/glossary/terms` `/summarize` `/process-session-file`, RPC `kb_page_text` |
| vue3 | `/knowledge/*`, `/folders/card`, `/documents/list`, `/artifact-url`, `/save-to-storage`, `/save-to-drive`, `/process`, `/process/drive/status`, `/parse/stored`, `/auth/google/*` |
| agent-sdk | `/retrieve` |
| office-mcp | `/documents/chunks-metadata` `/retrieve-by-indices` `/preview/pdf-highlight` |

## 스키마 전제

지도 API는 `sql/knowledge_doc_cards.sql`, `sql/knowledge_folder_cards.sql`, `sql/kb_mirror.sql`
을 전제로 한다. `/folders/tree`·`/folders/open` 이 `knowledge_files.has_text`/`page_count` 를
읽고, codex 미러가 `kb_page_text` RPC로 전문을 배치로 가져간다.
