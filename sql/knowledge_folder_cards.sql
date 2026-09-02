-- knowledge_folder_cards — 폴더 트리 네비게이션용 *계층 폴더 카드* 저장소.
--
-- 폴더 트리를 1급 인덱스로 삼는 knowledge-navigator(deepagents-lite)의 수집 단계 산출물.
-- 각 폴더에 대해 자식 doc_card(abstract) + 자식 폴더 카드를 bottom-up 으로 집계해
-- "이 폴더에 무엇이 있나"를 요약(카드)으로 남긴다. flat 임베딩이 cross-contamination 되는
-- 대규모(동일 골격의 여러 사업) 코퍼스에서 폴더 경로가 구별 신호이기 때문.
--
-- knowledge_folders(빈 폴더 등록)와 분리하는 이유: 카드가 필요한 폴더는 *파일 folder_path
-- 에서 파생된 폴더 전부*라 의미가 다르다. card 는 결정론 필드(수/날짜/종류/엔티티) + LLM
-- 요약(summary/topics)을 합친 JSONB. signature 로 증분 재생성 판정.
--
-- Supabase SQL 에디터에서 1회 실행. 이 테이블이 없어도 /folders/* 는 card=null 로 동작(Stage 1).

CREATE TABLE IF NOT EXISTS public.knowledge_folder_cards (
    tenant_id   text        NOT NULL,
    doc_role    text        NOT NULL DEFAULT 'content',
    folder_path text        NOT NULL,
    card        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    signature   text,
    built_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, doc_role, folder_path)
);

-- tenant 단위 조회(트리/오픈) 가속.
CREATE INDEX IF NOT EXISTS idx_kfc_tenant
    ON public.knowledge_folder_cards (tenant_id, doc_role);

-- 하위 폴더 prefix 조회용(증분 전파 시 자식 카드 모으기).
CREATE INDEX IF NOT EXISTS idx_kfc_tenant_path
    ON public.knowledge_folder_cards (tenant_id, doc_role, folder_path text_pattern_ops);
