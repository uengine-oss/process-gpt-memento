"""legal_review (검토 사례) doc_role 전용 인제스트.

변호사가 검토 코멘트를 단 과거 계약서(NDA/MOU 등) docx 를 조항 단위로 구조화하여,
[사업배경 청크 + 조항별 청크(메모 동승)] 로 임베딩한다. doc_role='legal_review'
업로드 시 knowledge_admin.upload_knowledge_file 에서 분기 호출.

POC: poc_nda/contract_structurer.py + gate_b*.py 에서 Gate A(구조화)·B(검색) 검증 완료.
"""
from app.services.legal_review.structurer import structure_contract
from app.services.legal_review.chunker import build_legal_review_documents

__all__ = ["structure_contract", "build_legal_review_documents"]
