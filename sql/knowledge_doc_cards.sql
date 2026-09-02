-- knowledge_doc_cards — 문서 카드(리트리벌 표면) 저장소.
--
-- 기존 knowledge_files.doc_card 는 앞 3쪽 + 뒤 1쪽만 보고 만든 abstract 한 줄이라
-- "이 문서를 열어야 하는가"를 답하지 못한다. 이 테이블의 card 는 문서 전문을 길이 기반
-- 슬라이딩 윈도우로 읽어 만든 JSONB 로, title/summary/doc_type/topics/entities/keywords/
-- answers_questions/coverage 를 담는다. coverage 가 있어 부분 읽음이 드러난다.
--
-- 카드 생성은 인제스트를 막지 않는다(백그라운드). status 로 진행 상태를 드러내고,
-- codex 미러는 카드가 pending 이어도 페이지 본문만으로 동작한다.
--
-- content_sha256: 같은 내용 문서를 다시 올렸을 때 카드를 재사용하기 위한 키.
--
-- doc_card 를 계속 쓰는 화면이 있으므로 knowledge_files.doc_card 는 그대로 둔다
-- (이 테이블이 없으면 서비스가 doc_card 로 폴백한다).
--
-- Supabase SQL 에디터에서 1회 실행. 멱등.

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
CREATE TABLE IF NOT EXISTS public.knowledge_doc_cards (
    tenant_id      text        NOT NULL,
    file_id        text        NOT NULL,
    card           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    signature      text,
    status         text        NOT NULL DEFAULT 'pending',
    content_sha256 text,
    built_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, file_id)
);

-- 스코프 조회(미러 준비)에서 file_id 배치로 끌어온다.
CREATE INDEX IF NOT EXISTS idx_kdc_tenant
    ON public.knowledge_doc_cards (tenant_id, status);

-- 같은 내용 문서의 카드 재사용 조회.
CREATE INDEX IF NOT EXISTS idx_kdc_content
    ON public.knowledge_doc_cards (tenant_id, content_sha256);
