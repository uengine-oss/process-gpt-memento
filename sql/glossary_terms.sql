-- glossary_terms — 구조화 용어사전(영문/한글뜻/약어) 저장 테이블.
--
-- 배경: 기존 glossary 경로는 doc_role='glossary' 자료를 LLM 으로 정제해
-- knowledge_files.glossary_compact(TEXT 덩어리)에 넣고, 채팅 진입 시 프롬프트에 통째로
-- prepend 했다. 사전이 커지면 컨텍스트를 잡아먹고, 번역 파이프라인(rfi-translate)처럼
-- '실제 등장한 용어만 골라 고정 번역'해야 하는 소비자에겐 못 쓴다.
--
-- 이 테이블은 고객이 준 고정 형식 CSV(헤더: 영문,한글뜻,약어)를 *행 단위*로 그대로 저장한다.
--   english      : 영문 표제어 (검색 키)
--   korean       : 한글뜻   (번역/정답 값)
--   abbreviation : 약어     (보조 검색 키, 비어 있을 수 있음)
--   english_norm : 소문자 + 비영숫자 공백화 정규화 (등장 스캔용)
--   abbr_norm    : 대문자 + 영숫자만 (약어 정확 매칭용)
-- file_id 는 knowledge_files.source_ref(=storage_path). 재업로드 시 file_id 로 지우고 다시 넣는다.
--
-- Supabase SQL 에디터에서 1회 실행.

CREATE TABLE IF NOT EXISTS public.glossary_terms (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id    text        NOT NULL,
    file_id      text        NOT NULL,           -- knowledge_files.source_ref (storage_path)
    file_name    text        NOT NULL DEFAULT '',
    english      text        NOT NULL DEFAULT '',
    korean       text        NOT NULL DEFAULT '',
    abbreviation text        NOT NULL DEFAULT '',
    english_norm text        NOT NULL DEFAULT '',
    abbr_norm    text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- tenant 전체 용어 로드(term-lock 매처 빌드) + file 단위 replace.
CREATE INDEX IF NOT EXISTS idx_glossary_terms_tenant
    ON public.glossary_terms (tenant_id);
CREATE INDEX IF NOT EXISTS idx_glossary_terms_file
    ON public.glossary_terms (tenant_id, file_id);

-- 정규화 키 기반 조회(향후 단건 검색 API 대비).
CREATE INDEX IF NOT EXISTS idx_glossary_terms_en_norm
    ON public.glossary_terms (tenant_id, english_norm);
CREATE INDEX IF NOT EXISTS idx_glossary_terms_abbr_norm
    ON public.glossary_terms (tenant_id, abbr_norm);
