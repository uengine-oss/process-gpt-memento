-- document_blocks — 문서의 인용 앵커 단위(문단·표 행 묶음·그림 설명).
--
-- 흐르는 문서(DOCX·HWPX)는 page_number 가 null 이다. 쪽이 있는 문서(PDF·PPTX·XLSX)만 쪽을 단다.
-- heading_level 은 파일에 명시된 헤딩(DOCX 제목 스타일·outlineLvl, HWPX 개요 수준)만.
-- 계약: docs/specs/knowledge-map.md
--
-- Supabase SQL 에디터에서 1회 실행(멱등).

CREATE TABLE IF NOT EXISTS public.document_blocks (
    tenant_id     text     NOT NULL,
    file_id       text     NOT NULL,
    block_index   integer  NOT NULL,
    kind          text     NOT NULL CHECK (kind IN ('paragraph', 'table', 'image')),
    text          text     NOT NULL,
    heading_level smallint,
    page_number   integer,
    bbox          jsonb,
    PRIMARY KEY (tenant_id, file_id, block_index)
);

-- 어떤 파서 버전으로 처리됐는가 — 파서가 바뀌면 이 값으로 재인덱싱 대상을 고른다.
ALTER TABLE public.knowledge_files ADD COLUMN IF NOT EXISTS parser_version text;
