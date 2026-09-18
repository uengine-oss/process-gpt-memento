# 지식베이스 = 지도

## 왜 벡터 RAG 가 아닌가

지식베이스의 소비처는 codex 다. codex 는 파일을 직접 읽고 `rg` 로 뒤질 수 있는 에이전트라,
서버가 top-k 청크를 골라 주는 것보다 **어디에 무엇이 있고 무엇부터 읽어야 하는지** 를 주는
쪽이 낫다. 채팅방에 폴더를 업로드하는 것과의 차이도 여기서 난다 — 업로드는 파일 더미고,
지식베이스는 미리 만들어 둔 지도다.

이 결정은 밖에서도 같은 방향으로 굳었다. Claude Code 는 2025-05 에 임베딩·벡터DB·청킹을
걷어내고 grep 으로 바꿨고, Cursor·Windsurf·Cline·Devin 도 같은 길을 갔다. Corpus2Skill
(*Don't Retrieve, Navigate*, arXiv 2604.14572) 은 코퍼스를 계층 디렉터리로 컴파일해
`SKILL.md`/`INDEX.md` 로 내려가는 방식이 dense retrieval 대비 Token F1 +29%, RAPTOR 대비
Factuality 0.675→0.739 를 보였다. 우리는 사업별 실제 폴더가 있어 그 논문이 K-Means 로
지어내는 계층을 이미 갖고 있다.

## 구성

| 층 | 저장소 | 만드는 곳 | 읽는 곳 |
|---|---|---|---|
| 원문 페이지 | `document_pages` | `document_pages.save_pages` | codex 미러 `text/`, `/document/page`, `/document/grep` |
| 문서 카드 | `knowledge_doc_cards` | `doc_cards.build_card` (백그라운드) | `CATALOG.tsv`, `/catalog`, `/folders/open` |
| 폴더 카드 | `knowledge_folder_cards` | `folder_cards.build_folder_card` (bottom-up, 폴더당 LLM 1회) | `TREE.md`, `/folders/tree`, `/folders/open` |
| 검색 힌트 | Chroma + `documents` | `rag_chain.process_and_store_documents` | `HINTS.md`, `/search` |

인제스트 성공 기준은 **페이지 저장** 이다. 페이지가 있으면 에이전트가 그 문서를 읽을 수
있다. 카드는 그 뒤에 백그라운드로 만들어지고, 벡터 인덱스는 실패해도 `index_status` 를
되돌리지 않고 `index_error` 에 `hints: ...` 로만 남는다.

## 카드가 답해야 하는 질문

문서 카드는 "무엇에 대한 문서인가"가 아니라 **"이 문서를 열어야 하는가"** 를 답한다.
그래서 `answers_questions` 가 리트리벌 표면이고, 여기에 `distinguishers` 를 더했다.

동일 골격의 사업 문서가 여러 벌인 코퍼스에서 문서 하나만 보고 쓴 요약은 "○○사업
제안요청서"로 수렴한다. Corpus2Skill 도 동질적 코퍼스에서 "cluster summaries collapse onto
near-duplicate labels" 로 졌다. 그래서 카드를 만들 때 같은 폴더의 다른 문서 제목을 함께
보여 주고, summary 첫 문장과 `distinguishers` 에 옆 문서와 구별되는 사실(사업명·발주처·
상대방·연도·차수·버전)을 쓰게 한다 (`doc_cards.CardContext`).

폴더 카드는 Corpus2Skill 의 `SKILL.md` 역할이다. `summary`/`topics` 에 더해
`reading_guide`(어떤 질문이면 무엇부터 열지)와 `start_with`(맥락을 가장 빨리 잡는 문서)를
담는다. `start_with` 는 실제 파일명만 남긴다 — 이름이 틀리면 화면도 에이전트도 그 문서로
못 간다.

## 준비 상태 (readiness)

지도가 얼마나 채워졌는지는 문서 단위로 `ready / pending / failed / no_text` 네 상태로
본다 (`folders._doc_state`). `no_text` 는 스캔본처럼 읽을 수 없는 문서라 "자료에 없음"과
다른 결론이다. `/folders/tree` 가 폴더별로 집계해 돌려주고, 관리 화면은 이 숫자를 그대로
쓴다 — 화면과 에이전트가 같은 지도를 본다는 원칙.

## grep 의 한계와 힌트

grep 은 어휘가 어긋나면 0 을 돌려준다. 한국어는 조사가 붙고 띄어쓰기가 흔들려 이게 더
심하다(`계약금액` 으로는 `계약 금액은` 을 못 찾는다). 그래서 벡터 검색을 완전히 버리지
않고 진입점 힌트(`HINTS.md`)로 남긴다. 반대로 서버 측 쿼리 확장(HyDE·multi-query·RAG
fusion)은 제거했다 — 질문을 고쳐 가며 다시 찾는 일은 에이전트 루프가 하는 일이다.

## 하지 않은 것

- **개인/공용 공간 분리.** 지도의 값어치는 완결성에서 나오는데 공간을 쪼개면 사람마다
  지도가 달라진다. 필요가 분명해지면 폴더 속성 하나로 시작한다.
- **청킹 전략 변경.** 청크는 힌트와 `/documents/full-text` 에만 쓰이므로 기본값을 건드릴
  이유가 없다. 바꾸려면 `benchmark/` 로 수치를 먼저 낸다.
