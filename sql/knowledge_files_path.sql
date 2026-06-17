-- knowledge_files.path — 문서의 *전체 상대경로* 핸들.
--
-- `path = folder_path + '/' + file_name(basename)` (folder_path 비면 file_name).
-- 에이전트(knowledge-navigator)가 grep/read 시 다루는 *단일 식별자*. 기존엔 file_name + folder_path
-- 두 조각을 약한 모델이 재조합하다 폴더를 한 단계 잘못 짚어 resolve 실패했음 → 도구가 path 하나를
-- 돌려주고 모델은 그대로 복사하게 만들어 그 슬립을 제거한다. 전체경로는 사업별로 유일하므로 동명
-- 파일 충돌도 자동 해결.
--
-- 유일성 강제 안 함(재업로드/중복 시 most-recent 선택). NOT NULL 아님(레거시 행은 NULL → resolve 가
-- basename 폴백으로 흡수). Supabase SQL 에디터에서 1회 실행. (기존 데이터는 삭제 후 재업로드 → 자동 채움)

ALTER TABLE public.knowledge_files ADD COLUMN IF NOT EXISTS path text;

-- tenant 내 path 정확 매칭 조회 가속 (_resolve_file_id 의 .eq("path", ...)).
CREATE INDEX IF NOT EXISTS idx_kf_tenant_path
    ON public.knowledge_files (tenant_id, path);
