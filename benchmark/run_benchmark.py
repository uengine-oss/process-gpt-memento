"""
RAG 벤치마크 러너.

사용법:
  1) benchmark/docs/ 폴더에 벤치마크하고 싶은 문서를 넣는다(PDF/DOCX/PPTX/XLSX/TXT/HWP/HWPX).
  2) 프로젝트 루트(process-gpt-memento/)의 .env가 로드된 상태에서 실행한다:
       python -m benchmark.run_benchmark
     또는
       cd benchmark && python run_benchmark.py

플로우:
  문서 파싱 → LLM으로 QA셋 자동 생성 (3페이지 윈도우 병렬)
  → 청커 전략별로 별도 Chroma 컬렉션에 임베딩
  → 청커×리트리버 조합별로 QA를 실행하고 LLM judge로 채점
  → 조합별 점수 매트릭스 출력 + results/<timestamp>/ 에 JSON 저장

주의:
  이 스크립트는 서비스 운영 DB(Supabase/Chroma)와 완전히 분리된
  benchmark/chroma_bench/ 디렉토리만 사용한다.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List

# benchmark/ 에서 실행해도, 프로젝트 루트에서 실행해도 되도록 path 조정
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from dotenv import load_dotenv
from langchain.schema import Document

load_dotenv(_ROOT / ".env", override=False)

from chunkers import available_strategies as chunker_strategies, get_chunker  # noqa: E402
from retrievers import available_strategies as retriever_strategies, get_retriever  # noqa: E402
from llm import create_llm, create_embeddings  # noqa: E402
from document_loader import DocumentProcessor  # noqa: E402

from bench_store import BenchVectorStore  # noqa: E402
from qa import QAItem, generate_qa_set, judge_answer, retrieval_recall  # noqa: E402


# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

DOCS_DIR = _HERE / "docs"
CHROMA_DIR = _HERE / "chroma_bench"
RESULTS_DIR = _HERE / "results"

TOP_K = 5
# 리트리버가 너무 많으면 LLM 호출 폭증. 원하면 여기서 추려라.
CHUNKER_STRATEGIES = chunker_strategies()      # recursive, fixed_token, markdown_header, semantic, hybrid
RETRIEVER_STRATEGIES = retriever_strategies()  # plain, multi_query, hyde, rag_fusion, rewrite

SUPPORTED_EXT = {".pdf", ".docx", ".pptx", ".xlsx", ".txt", ".hwp", ".hwpx"}


# ---------------------------------------------------------------------------
# RAG 답변 (RAGChain을 쓰지 않고 최소 버전으로 재구성 — Supabase 의존성 회피)
# ---------------------------------------------------------------------------

_ANSWER_PROMPT_KO = """다음의 맥락을 사용하여 질문에 답변해주세요.
답을 모른다면, 모른다고 말씀해주세요. 답을 만들어내려고 하지 마세요.

맥락: {context}

질문: {question}

답변: """


def _format_context(docs: List[Document]) -> str:
    parts = []
    for i, d in enumerate(docs, 1):
        meta = d.metadata or {}
        header_bits = [f"문서 {i}"]
        if meta.get("section_title"):
            header_bits.append(f"섹션: {meta['section_title']}")
        if meta.get("page_number") is not None:
            header_bits.append(f"페이지: {meta['page_number']}")
        parts.append(" | ".join(header_bits) + "\n" + (d.page_content or ""))
    return "\n\n".join(parts)


async def rag_answer(llm, retriever, store: BenchVectorStore, query: str, top_k: int):
    docs = await retriever.retrieve(query, store, filter=None, top_k=top_k)
    if not docs:
        return "질문에 답변하기에 충분한 정보가 없습니다.", []
    prompt = _ANSWER_PROMPT_KO.format(context=_format_context(docs), question=query)
    resp = await llm.ainvoke(prompt)
    answer = getattr(resp, "content", resp)
    if not isinstance(answer, str):
        answer = str(answer)
    return answer, docs


# ---------------------------------------------------------------------------
# 문서 로드 & 청킹
# ---------------------------------------------------------------------------

async def load_document(file_path: Path) -> List[Document]:
    """DocumentProcessor.load_document 로 파싱만 수행 (청킹은 별도)."""
    processor = DocumentProcessor()  # chunker는 여기선 안 쓰고 로더만 사용
    with open(file_path, "rb") as f:
        import io
        buf = io.BytesIO(f.read())
    docs = await processor.load_document(buf, file_path.name)
    return docs or []


async def chunk_documents(strategy: str, documents: List[Document]) -> List[Document]:
    """전략별 청커로 분할하고, page_number 같은 필수 메타데이터를 보강한다."""
    chunker = get_chunker(strategy=strategy)
    chunks = await chunker.split(documents)

    for i, c in enumerate(chunks):
        c.metadata = dict(c.metadata or {})
        c.metadata["chunk_index"] = i
        # PDF 등: 0-based `page`가 있으면 1-based `page_number` 추가 (recall 계산용)
        p = c.metadata.get("page")
        if p is not None and "page_number" not in c.metadata:
            try:
                c.metadata["page_number"] = int(p) + 1
            except (TypeError, ValueError):
                pass
        c.metadata.setdefault("chunker_name", strategy)
    return chunks


# ---------------------------------------------------------------------------
# 단일 문서 벤치마크
# ---------------------------------------------------------------------------

@dataclass
class CombinationResult:
    chunker: str
    retriever: str
    num_qa: int
    avg_score: float        # LLM judge 0~5 평균
    avg_recall: float       # retrieval recall (source_pages 기준) 0~1 평균
    elapsed_sec: float
    per_qa: List[Dict[str, Any]]


async def benchmark_file(file_path: Path, llm, embeddings, out_dir: Path) -> Dict[str, Any]:
    print(f"\n{'#' * 70}\n# Benchmarking: {file_path.name}\n{'#' * 70}")

    # 1. 파싱
    documents = await load_document(file_path)
    if not documents:
        print(f"[skip] 파싱 실패: {file_path.name}")
        return {"file": file_path.name, "error": "parse_failed"}

    # PDF 등: page 메타데이터가 비어있으면 인덱스를 page로 박아준다
    for idx, d in enumerate(documents):
        d.metadata = dict(d.metadata or {})
        d.metadata.setdefault("file_name", file_path.name)
        if "page" not in d.metadata and len(documents) > 1:
            d.metadata["page"] = idx

    print(f"[parse] {len(documents)} Document chunk(s) parsed")

    # 2. QA셋 생성
    qa_items: List[QAItem] = await generate_qa_set(llm, documents)
    if not qa_items:
        print(f"[skip] QA 생성 실패: {file_path.name}")
        return {"file": file_path.name, "error": "qa_generation_failed"}

    # QA셋 저장
    (out_dir / f"{file_path.stem}.qa.json").write_text(
        json.dumps([q.to_dict() for q in qa_items], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # 3. 청커별 인덱싱 (전략당 1회만)
    stores: Dict[str, BenchVectorStore] = {}
    for chunker_name in CHUNKER_STRATEGIES:
        print(f"\n[chunk+embed] strategy={chunker_name}")
        try:
            chunks = await chunk_documents(chunker_name, documents)
        except Exception as e:
            print(f"  [chunk] failed ({chunker_name}): {e}")
            continue
        if not chunks:
            print(f"  [chunk] empty: {chunker_name}")
            continue
        print(f"  {len(chunks)} chunks")

        collection = f"bench_{file_path.stem}_{chunker_name}"
        # Chroma 컬렉션명 규칙: 영문/숫자/._- 만, 3~63자
        collection = _safe_collection_name(collection)
        store = BenchVectorStore(
            persist_dir=str(CHROMA_DIR),
            collection_name=collection,
            embeddings=embeddings,
        )
        try:
            store.add_documents(chunks)
            stores[chunker_name] = store
        except Exception as e:
            print(f"  [embed] failed ({chunker_name}): {e}")

    # 4. 조합별 평가
    results: List[CombinationResult] = []
    for chunker_name, store in stores.items():
        for retriever_name in RETRIEVER_STRATEGIES:
            print(f"\n[run] chunker={chunker_name} x retriever={retriever_name}")
            t0 = time.time()
            retriever = get_retriever(strategy=retriever_name, top_k=TOP_K)

            per_qa: List[Dict[str, Any]] = []
            scores: List[float] = []
            recalls: List[float] = []

            # 조합당 QA를 약간 병렬로 처리 (너무 세게 돌리면 rate limit)
            sem = asyncio.Semaphore(3)

            async def run_one(qa: QAItem):
                async with sem:
                    try:
                        answer, docs = await rag_answer(
                            llm, retriever, store, qa.question, TOP_K
                        )
                    except Exception as e:
                        print(f"    [rag] error: {e}")
                        answer, docs = "", []
                    recall = retrieval_recall(docs, qa.source_pages)
                    judgement = await judge_answer(
                        llm, qa.question, qa.expected_answer, answer
                    )
                    return {
                        "question": qa.question,
                        "expected": qa.expected_answer,
                        "actual": answer,
                        "source_pages": qa.source_pages,
                        "retrieved_pages": [
                            (d.metadata or {}).get("page_number") for d in docs
                        ],
                        "score": judgement["score"],
                        "judge_reason": judgement["reason"],
                        "recall": recall,
                    }

            per_qa = await asyncio.gather(*[run_one(q) for q in qa_items])
            scores = [p["score"] for p in per_qa]
            recalls = [p["recall"] for p in per_qa]

            res = CombinationResult(
                chunker=chunker_name,
                retriever=retriever_name,
                num_qa=len(per_qa),
                avg_score=round(sum(scores) / len(scores), 3) if scores else 0.0,
                avg_recall=round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
                elapsed_sec=round(time.time() - t0, 2),
                per_qa=per_qa,
            )
            results.append(res)
            print(
                f"  -> score={res.avg_score}/5  recall={res.avg_recall}  "
                f"({res.elapsed_sec}s)"
            )

    # 5. 결과 저장 & 표 출력
    report = {
        "file": file_path.name,
        "num_qa": len(qa_items),
        "combinations": [asdict(r) for r in results],
    }
    (out_dir / f"{file_path.stem}.report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _print_matrix(file_path.name, results)
    return report


def _safe_collection_name(name: str) -> str:
    import re
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    if len(cleaned) < 3:
        cleaned = (cleaned + "_bench")[:3]
    return cleaned[:63]


def _print_matrix(file_name: str, results: List[CombinationResult]) -> None:
    print(f"\n{'=' * 70}\n결과 매트릭스 — {file_name}\n{'=' * 70}")
    # 두 개 표: score / recall
    chunkers = sorted({r.chunker for r in results})
    retrievers = sorted({r.retriever for r in results})
    lookup = {(r.chunker, r.retriever): r for r in results}

    for title, key in (("Score (LLM judge, 0~5)", "avg_score"),
                       ("Retrieval Recall (0~1)", "avg_recall")):
        print(f"\n[{title}]")
        header = ["chunker \\ retriever"] + retrievers
        rows = [header]
        for c in chunkers:
            row = [c]
            for r in retrievers:
                v = lookup.get((c, r))
                row.append(f"{getattr(v, key):.3f}" if v else "-")
            rows.append(row)
        widths = [max(len(str(r[i])) for r in rows) for i in range(len(rows[0]))]
        for row in rows:
            print("  " + " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))

    # 종합 랭킹: score 우선, 동점 시 recall
    print("\n[Top combinations]")
    ranked = sorted(results, key=lambda r: (r.avg_score, r.avg_recall), reverse=True)
    for i, r in enumerate(ranked[:5], 1):
        print(f"  {i}. {r.chunker:18s} + {r.retriever:12s}  "
              f"score={r.avg_score:.3f}  recall={r.avg_recall:.3f}")


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

async def main():
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(
        p for p in DOCS_DIR.iterdir()
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXT
    )
    if not files:
        print(f"[error] {DOCS_DIR} 에 벤치마크할 문서가 없습니다.")
        print(f"        지원 확장자: {sorted(SUPPORTED_EXT)}")
        return

    print(f"[init] docs: {[f.name for f in files]}")
    print(f"[init] chunkers : {CHUNKER_STRATEGIES}")
    print(f"[init] retrievers: {RETRIEVER_STRATEGIES}")

    llm = create_llm(temperature=0.0)
    embeddings = create_embeddings()

    run_id = time.strftime("%Y%m%d_%H%M%S")
    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[init] results → {out_dir}")

    all_reports = []
    for f in files:
        try:
            report = await benchmark_file(f, llm, embeddings, out_dir)
            all_reports.append(report)
        except Exception as e:
            print(f"[error] {f.name}: {e}")
            import traceback; traceback.print_exc()
            all_reports.append({"file": f.name, "error": str(e)})

    (out_dir / "summary.json").write_text(
        json.dumps(all_reports, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\n[done] summary saved to {out_dir / 'summary.json'}")


if __name__ == "__main__":
    asyncio.run(main())
