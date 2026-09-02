-- 지식베이스 조회/삭제 성능 인덱스
-- =============================================================================
-- 증상: 파일 5,000개 이상에서 (1) 지식베이스 페이지/모달 조회가 느리고
--       (2) 문서·폴더 삭제가 매우 오래 걸림.
--
-- 원인: documents(=RAG 청크) 테이블의 삭제/조회 쿼리가 metadata->>'file_id' 등
--       JSONB 표현식으로 필터하는데 해당 표현식 인덱스가 없어 매번 Seq Scan(풀스캔).
--       documents 는 (파일수 × 파일당 청크수) 라 수십만~백만 row 규모 → 풀스캔 1회당 수백 ms~초.
--       파일 1개 삭제(delete_entry)가 이 풀스캔을 4~5회, 폴더 삭제는 파일마다 반복 → 폭발.
--
-- 조치: 삭제/조회가 실제로 거는 필터 표현식에 인덱스를 추가한다. 멱등(IF NOT EXISTS).
--
-- 참고(표준 Supabase 스택 기준 *이미 존재*하는 보조 인덱스 — 여기서 다시 만들지 않음):
--       document_pages(tenant_id, file_id)            = idx_document_pages_tenant_file
--       document_images(document_id)                  = idx_document_images_document_id
--       processed_files(file_id, tenant_id) UNIQUE    = processed_files_file_id_tenant_id_key
--       knowledge_files(tenant_id, folder_path)       = idx_knowledge_files_tenant_folder
--       knowledge_files(tenant_id, source_type, source_ref) UNIQUE = knowledge_files_unique_source
--       (위가 없는 환경이면 아래 "보조" 블록 주석을 풀어 함께 실행)
--
-- 실행: Supabase Studio 권한 이슈 시 DB 컨테이너에서 직접:
--       docker exec -i supabase_db_<project> psql -U postgres -d postgres < 이파일
--       documents 가 매우 크면(수백만 row) 쓰기 락 회피를 위해 각 CREATE INDEX 에
--       CONCURRENTLY 를 붙여 "한 문장씩" 실행할 것(트랜잭션 블록 밖에서만 가능).
-- =============================================================================

-- ── documents (RAG 청크 본문 + JSONB metadata) — 핵심: 현재 전부 누락 ────────
-- delete_entry / list 류가 거는 metadata->>'tenant_id' + metadata->>'file_id'
-- (청크 id 수집, 청크 본문 삭제, file_id 기준 청크 조회)
CREATE INDEX IF NOT EXISTS idx_documents_tenant_file
    ON public.documents ((metadata->>'tenant_id'), (metadata->>'file_id'));

-- file_name 기준 청크 조회 (get_chunks_by_indices / get_all_chunks_metadata)
CREATE INDEX IF NOT EXISTS idx_documents_tenant_filename
    ON public.documents ((metadata->>'tenant_id'), (metadata->>'file_name'));

-- image_analysis 행 역추적: type='image_analysis' AND document_id ∈ (부모 청크 id)
CREATE INDEX IF NOT EXISTS idx_documents_type_docid
    ON public.documents ((metadata->>'type'), (metadata->>'document_id'));

-- ── knowledge_files — 폴더 prefix LIKE 'folder/%' 가속 (없으면 추가) ─────────
-- folders_open / list_files_in_folder_recursive 의 왼쪽 고정 LIKE 패턴 매칭.
-- 기본 btree 는 LIKE 에 못 쓰이므로 text_pattern_ops 보조 인덱스를 둔다.
CREATE INDEX IF NOT EXISTS idx_kf_tenant_folder_pattern
    ON public.knowledge_files (tenant_id, folder_path text_pattern_ops);

-- ── 보조(표준 스택엔 이미 있음 — 없는 환경에서만 주석 해제) ─────────────────
-- CREATE INDEX IF NOT EXISTS idx_document_pages_tenant_file
--     ON public.document_pages (tenant_id, file_id);
-- CREATE INDEX IF NOT EXISTS idx_processed_files_tenant_file
--     ON public.processed_files (tenant_id, file_id);
-- CREATE INDEX IF NOT EXISTS idx_document_images_document_id
--     ON public.document_images (document_id);
-- CREATE INDEX IF NOT EXISTS idx_kf_tenant_folder
--     ON public.knowledge_files (tenant_id, folder_path);
-- CREATE INDEX IF NOT EXISTS idx_kf_tenant_srctype_ref
--     ON public.knowledge_files (tenant_id, source_type, source_ref);

-- 적용 후 확인용(선택):
--   EXPLAIN ANALYZE
--   SELECT id FROM public.documents
--   WHERE metadata->>'tenant_id' = '<tenant>' AND metadata->>'file_id' = '<ref>';
--   → "Index Scan using idx_documents_tenant_file" 이 보이면 정상.
