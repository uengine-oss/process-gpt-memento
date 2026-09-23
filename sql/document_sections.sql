-- document_sections — 블록 위에 얹는 탐색 단위(목차)와 섹션 키워드 검색.
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

-- 부분 문자열(ILIKE '%용어%') 검색 가속. 한국어는 형태소 분석 없이 부분 일치로 찾는다.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_document_blocks_text_trgm
    ON public.document_blocks USING gin (text gin_trgm_ops);

-- 섹션 키워드 검색. 용어마다 섹션 안 블록에서 몇 번 걸렸는지(tf)와 범위 안에서 몇 섹션에
-- 걸렸는지(df)로 BM25 모양의 점수를 매긴다. 섹션 제목·요약에 걸리면 한 번 더 센다.
CREATE OR REPLACE FUNCTION public.kb_section_keyword_search(
    p_tenant_id text,
    p_file_ids  text[],
    p_terms     text[],
    p_limit     integer DEFAULT 50
)
RETURNS TABLE (file_id text, section_index integer, score double precision, matched_terms text[])
LANGUAGE sql STABLE AS $$
    WITH scope AS (
        SELECT s.* FROM public.document_sections s
        WHERE s.tenant_id = p_tenant_id AND s.file_id = ANY(p_file_ids)
    ),
    block_hits AS (
        SELECT b.file_id, b.block_index, t.term
        FROM public.document_blocks b
        CROSS JOIN unnest(p_terms) AS t(term)
        WHERE b.tenant_id = p_tenant_id AND b.file_id = ANY(p_file_ids)
          AND b.text ILIKE '%' || t.term || '%'
    ),
    section_hits AS (
        SELECT s.file_id, s.section_index, h.term, count(*)::double precision AS tf
        FROM block_hits h
        JOIN scope s ON s.file_id = h.file_id AND h.block_index BETWEEN s.start_block AND s.end_block
        GROUP BY s.file_id, s.section_index, h.term
        UNION ALL
        SELECT s.file_id, s.section_index, t.term, 1.0
        FROM scope s CROSS JOIN unnest(p_terms) AS t(term)
        WHERE s.title ILIKE '%' || t.term || '%' OR s.summary ILIKE '%' || t.term || '%'
    ),
    per_term AS (
        SELECT h.file_id, h.section_index, h.term, sum(h.tf) AS tf
        FROM section_hits h GROUP BY h.file_id, h.section_index, h.term
    ),
    df AS (SELECT p.term, count(*)::double precision AS df FROM per_term p GROUP BY p.term),
    n AS (SELECT greatest(count(*), 1)::double precision AS n FROM scope)
    SELECT p.file_id, p.section_index,
           sum(ln((n.n - df.df + 0.5) / (df.df + 0.5) + 1.0) * (p.tf * 2.2) / (p.tf + 1.2)) AS score,
           array_agg(p.term ORDER BY p.term) AS matched_terms
    FROM per_term p JOIN df ON df.term = p.term CROSS JOIN n
    GROUP BY p.file_id, p.section_index
    ORDER BY score DESC
    LIMIT p_limit;
$$;
