-- document_sections — 블록 위에 얹는 탐색 단위(목차).
--
-- 섹션은 문서 전체를 빈틈없이 덮는다: 0번 섹션은 block 0 에서 시작하고, 각 섹션은 다음 섹션 직전 블록에서 끝난다.
-- source: llm(카드 LLM 이 고름) | split(크기 상한 때문에 블록 경계에서 자름).
-- 계약: docs/specs/knowledge-map.md
--
-- Supabase SQL 에디터에서 1회 실행(멱등). sql/document_blocks.sql 다음에.

CREATE TABLE IF NOT EXISTS public.document_sections (
    tenant_id     text     NOT NULL,
    file_id       text     NOT NULL,
    section_index integer  NOT NULL,
    start_block   integer  NOT NULL,
    end_block     integer  NOT NULL,
    title         text     NOT NULL,
    summary       text     NOT NULL DEFAULT '',
    chars         integer  NOT NULL,
    source        text     NOT NULL DEFAULT 'llm',
    PRIMARY KEY (tenant_id, file_id, section_index)
);
