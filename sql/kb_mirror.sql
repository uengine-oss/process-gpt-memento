-- codex 지식베이스 미러가 쓰는 스키마 보강. 멱등이며, 적용 전에도 미러는
-- 더 느린 경로(페이지 행 페이지네이션)로 동작한다.
--
-- 1) knowledge_files 결정론 지표
--    스캔본을 "자료에 없음" 이 아니라 "읽을 수 없음" 으로 구분하려면 카탈로그가
--    텍스트 유무를 알아야 한다. abstract_status 로 간접 추정하던 것을 사실로 바꾼다.
--
-- 2) kb_page_text(RPC)
--    미러 준비의 왕복 수를 *파일 수* 에서 *배치 수* 로 떨어뜨리는 핵심.
--    document_pages 를 파일별로 페이지 순서대로 이어 붙여 `=== page N ===` 마커가 박힌
--    전문을 한 번에 돌려준다. 마커는 에이전트의 인용 단위(page N)가 된다.

-- 적용 방법
--   Supabase SQL 에디터(Studio)에서 실행한다. 멱등이라 여러 번 돌려도 안전하다.
--
--   로컬에서 `ERROR: 42501 must be owner of table` 이 나면, 앱 테이블이 supabase_admin
--   소유인데 Studio 는 postgres 롤로 붙기 때문이다. DB 컨테이너에서 한 번만 멤버십을 주면
--   이후로는 Studio 에서 그대로 실행된다(로컬 개발 전용):
--     docker exec -e PGPASSWORD=<pw> <db-container> psql -U supabase_admin -d postgres
--       -c "GRANT supabase_admin TO postgres;"
--   되돌리려면 REVOKE supabase_admin FROM postgres;
--   새 테이블·함수가 REST 에 안 보이면: NOTIFY pgrst, 'reload schema';
--
ALTER TABLE public.knowledge_files
    ADD COLUMN IF NOT EXISTS has_text   boolean,
    ADD COLUMN IF NOT EXISTS text_chars integer,
    ADD COLUMN IF NOT EXISTS page_count integer;

-- 스코프 조회는 (tenant_id, folder_path) 로 훑는다.
CREATE INDEX IF NOT EXISTS idx_kf_tenant_folder
    ON public.knowledge_files (tenant_id, folder_path text_pattern_ops);

-- 페이지 본문 조회는 (tenant_id, file_id, page_number) 정렬로만 쓰인다.
CREATE INDEX IF NOT EXISTS idx_dp_tenant_file_page
    ON public.document_pages (tenant_id, file_id, page_number);

CREATE OR REPLACE FUNCTION public.kb_page_text(p_tenant_id text, p_file_ids text[])
RETURNS TABLE (file_id text, full_text text, page_count integer)
LANGUAGE sql
STABLE
AS $$
    SELECT
        dp.file_id,
        string_agg('=== page ' || dp.page_number || ' ===' || E'\n' || dp.content,
                   E'\n\n' ORDER BY dp.page_number) AS full_text,
        count(*)::int AS page_count
    FROM public.document_pages dp
    WHERE dp.tenant_id = p_tenant_id
      AND dp.file_id = ANY(p_file_ids)
      AND coalesce(btrim(dp.content), '') <> ''
    GROUP BY dp.file_id;
$$;

COMMENT ON FUNCTION public.kb_page_text(text, text[]) IS
    'codex KB 미러용 — 파일별 전문을 페이지 마커와 함께 배치로 반환';
