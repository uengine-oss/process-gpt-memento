"""벡터 인덱스 백엔드 추상화 (Chroma / Qdrant).

``VectorStoreManager`` 는 Supabase(원본 문서) + 벡터 인덱스(임베딩)의 조합인데, 그중
*벡터 인덱스* 쪽만 이 모듈로 분리해 백엔드를 갈아끼울 수 있게 한다.

필터 표현식은 **Chroma where 문법을 공용 언어로** 쓴다. 호출부(knowledge_files 등)가 이미
``{"$and": [{"tenant_id": t}, {"file_id": {"$in": [...]}}]}`` 형태를 직접 만들어 넘기고 있어,
그 문법을 그대로 유지하는 편이 호출부를 건드리지 않는다. Qdrant 백엔드는 이 표현식을
자기 ``Filter`` 로 번역한다.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence
import uuid

from app.core import config


# 필터에 실제로 쓰이는 metadata 키. Qdrant 는 payload 인덱스가 없는 키로 필터하면
# 세그먼트를 전수 스캔하므로(on_disk_payload 면 디스크까지 훑는다) 반드시 인덱싱한다.
# 출처: app/api/retrieve.py, app/api/folders.py, app/api/legal_review.py,
#       app/services/knowledge_files.py 의 필터 구성부.
#
# ★ 여기에 *쓰지도 않는 키*를 넣지 말 것. payload 인덱스는 RAM 에 상주하고, 값이 고유할수록
#   (포인트당 1개) 비싸다. file_name/file_path/document_row_id 는 Supabase 조회에만 쓰이고
#   벡터 검색 필터로는 한 번도 안 쓰여서 제외했다 — 특히 document_row_id 는 포인트마다 값이
#   달라 인덱싱 비용이 가장 크다(= point id 그 자체라 필터할 이유도 없다).
FILTERABLE_KEYS: tuple[str, ...] = (
    "tenant_id",
    "file_id",
    "chunk_id",  # exclude_chunk_ids ($nin) 전용 — 값이 고유하지만 이건 실제로 필요하다
    "room_id",
    "drive_folder_id",
    "knowledge_scope",
    "proc_inst_id",
    "source_type",
    "type",
    "contract_type",
)

# 원본 id 가 UUID 가 아닐 때만 payload 에 넣어두는 보존 키 (Qdrant point id 는 UUID/uint 만 허용).
_RAW_ID_KEY = "__raw_id__"


class VectorIndex(Protocol):
    """임베딩 인덱스가 제공해야 하는 최소 표면."""

    #: 백엔드가 단일 writer 라 상위에서 쓰기를 직렬화해야 하는지.
    requires_write_lock: bool

    def upsert(
        self,
        ids: Sequence[str],
        embeddings: Sequence[Sequence[float]],
        documents: Sequence[str],
        metadatas: Sequence[Dict[str, Any]],
    ) -> None: ...

    def query(
        self,
        embedding: Sequence[float],
        top_k: int,
        where: Optional[Dict[str, Any]] = None,
        with_metadata: bool = False,
    ) -> List[Dict[str, Any]]:
        """유사도 내림차순 히트. 각 원소는 ``{"id": str, "metadata": dict | None}``."""
        ...

    def delete_where(self, where: Dict[str, Any]) -> None: ...

    def delete_ids(self, ids: Sequence[str]) -> None: ...

    def get_embeddings(self, ids: Sequence[str]) -> Dict[str, List[float]]: ...

    def count(self) -> int: ...


# ──────────────────────────────── Chroma ────────────────────────────────


class ChromaIndex:
    """기존 Chroma 경로 — 서버 모드(HttpClient) 우선, 없으면 in-process PersistentClient."""

    # SQLite 백엔드라 동시 쓰기가 "database is locked" 를 유발한다.
    requires_write_lock = True

    def __init__(self) -> None:
        from pathlib import Path

        self.collection_name = config.chroma_collection_name().strip()

        server_host = config.chroma_server_host()
        if server_host:
            import chromadb

            server_port = config.chroma_server_port()
            self.client = chromadb.HttpClient(host=server_host, port=server_port)
            print(
                f"[vector_index] Chroma 서버 모드: http://{server_host}:{server_port}",
                flush=True,
            )
        else:
            from chromadb import PersistentClient

            persist_dir = Path(config.chroma_persist_directory()).expanduser()
            if not persist_dir.is_absolute():
                # app/services/vector_index.py → repo root
                repo_root = Path(__file__).resolve().parents[2]
                persist_dir = (repo_root / persist_dir).resolve()
            persist_dir.mkdir(parents=True, exist_ok=True)
            self.client = PersistentClient(path=str(persist_dir))
            print(f"[vector_index] Chroma in-process 모드: {persist_dir}", flush=True)

        self.collection = self.client.get_or_create_collection(
            name=self.collection_name,
            metadata={"hnsw:space": "cosine"},
        )

    def upsert(self, ids, embeddings, documents, metadatas) -> None:
        self.collection.upsert(
            ids=list(ids),
            embeddings=list(embeddings),
            documents=list(documents),
            metadatas=list(metadatas),
        )

    def query(self, embedding, top_k, where=None, with_metadata=False):
        kwargs: Dict[str, Any] = {
            "query_embeddings": [list(embedding)],
            "n_results": max(1, int(top_k)),
            "where": where,
        }
        if with_metadata:
            kwargs["include"] = ["metadatas"]
        response = self.collection.query(**kwargs)

        id_groups = response.get("ids") or []
        ids = list(id_groups[0]) if id_groups else []
        meta_groups = response.get("metadatas") or []
        metas = list(meta_groups[0]) if (with_metadata and meta_groups) else []

        hits: List[Dict[str, Any]] = []
        for i, raw_id in enumerate(ids):
            meta = metas[i] if i < len(metas) else None
            hits.append({"id": str(raw_id), "metadata": meta if isinstance(meta, dict) else None})
        return hits

    def delete_where(self, where: Dict[str, Any]) -> None:
        self.collection.delete(where=where)

    def delete_ids(self, ids: Sequence[str]) -> None:
        self.collection.delete(ids=list(ids))

    def get_embeddings(self, ids: Sequence[str]) -> Dict[str, List[float]]:
        fetched = self.collection.get(ids=list(ids), include=["embeddings"])
        out: Dict[str, List[float]] = {}
        got_ids = list(fetched.get("ids") or [])
        got_embs = fetched.get("embeddings")
        got_embs = list(got_embs) if got_embs is not None else []
        for i, rid in enumerate(got_ids):
            if i < len(got_embs) and got_embs[i] is not None:
                out[str(rid)] = list(got_embs[i])
        return out

    def count(self) -> int:
        return int(self.collection.count())


# ──────────────────────────────── Qdrant ────────────────────────────────


def _to_point_id(raw: str) -> str:
    """Qdrant point id 는 UUID 또는 uint 만 허용 — 비 UUID 문자열은 uuid5 로 사상."""
    try:
        return str(uuid.UUID(str(raw)))
    except (ValueError, AttributeError, TypeError):
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"memento:{raw}"))


class QdrantIndex:
    """Qdrant 백엔드 — 메모리 상주분을 양자화본으로 줄이는 구성.

    메모리 설계(대용량 폐쇄망 인덱스가 이 백엔드를 쓰는 이유):
      - ``vectors.on_disk``      : 원본 float32 벡터를 디스크에 두고 mmap. 재채점 때만 읽는다.
      - ``hnsw_config.on_disk``  : HNSW 그래프도 mmap — 그래프 자체가 수 GB 가 되면 유효.
      - ``quantization always_ram``: int8 양자화본만 RAM 상주. 벡터당 1/4 크기.
      - ``on_disk_payload``      : metadata 를 디스크에. 필터는 payload *인덱스*(RAM)로 처리.
    검색은 RAM 의 양자화본으로 후보를 넓게(oversampling) 뽑고, 디스크의 원본으로 재채점해
    정확도를 회복한다.
    """

    # Qdrant 는 동시 쓰기를 자체 처리한다 — 상위 직렬화 락이 불필요(처리량 손해만 남음).
    requires_write_lock = False

    def __init__(self) -> None:
        from qdrant_client import QdrantClient

        self.collection_name = config.qdrant_collection_name().strip()
        self.url = config.qdrant_url()
        self.on_disk = config.qdrant_on_disk()
        self.quantization = config.qdrant_quantization()
        self.oversampling = config.qdrant_search_oversampling()
        self.hnsw_ef = config.qdrant_hnsw_ef()

        self.client = QdrantClient(
            url=self.url,
            api_key=config.qdrant_api_key(),
            # 대용량 컬렉션의 첫 조회/최적화 중 응답이 늦을 수 있어 넉넉히.
            timeout=120,
        )
        print(
            f"[vector_index] Qdrant 모드: {self.url} collection={self.collection_name!r} "
            f"on_disk={self.on_disk} quantization={self.quantization}",
            flush=True,
        )
        self.ensure_collection()

    # ── 스키마 ──

    def ensure_collection(self, vector_size: Optional[int] = None) -> None:
        """컬렉션과 payload 인덱스를 멱등하게 보장."""
        from qdrant_client import models

        size = int(vector_size or config.qdrant_vector_size())

        if not self.client.collection_exists(self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=size,
                    distance=models.Distance.COSINE,
                    on_disk=self.on_disk,
                ),
                hnsw_config=models.HnswConfigDiff(
                    # Chroma 쪽 max_neighbors=16 / ef_construction=100 과 동일하게 맞춰
                    # 이관 전후 리콜 특성이 달라지지 않게 한다.
                    m=16,
                    ef_construct=100,
                    on_disk=self.on_disk,
                ),
                quantization_config=self._quantization_config(),
                on_disk_payload=self.on_disk,
            )
            print(
                f"[vector_index] Qdrant 컬렉션 생성: {self.collection_name} (dim={size})",
                flush=True,
            )

        for key in FILTERABLE_KEYS:
            try:
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=key,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                    wait=True,
                )
            except Exception:
                # 이미 존재하면 409 — 멱등 보장을 위해 삼킨다.
                pass

    def _quantization_config(self):
        from qdrant_client import models

        if self.quantization == "none":
            return None
        if self.quantization == "binary":
            return models.BinaryQuantization(
                binary=models.BinaryQuantizationConfig(always_ram=True)
            )
        return models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(
                type=models.ScalarType.INT8,
                # 상하위 1% 이상치를 잘라 int8 해상도를 실제 분포에 집중시킨다.
                quantile=0.99,
                always_ram=True,
            )
        )

    # ── 쓰기 ──

    def upsert(self, ids, embeddings, documents, metadatas, wait: bool = True) -> None:
        from qdrant_client import models

        points: List[Any] = []
        for raw_id, vector, document, metadata in zip(ids, embeddings, documents, metadatas):
            payload = dict(metadata or {})
            point_id = _to_point_id(raw_id)
            if point_id != str(raw_id):
                payload[_RAW_ID_KEY] = str(raw_id)
            # Chroma 는 document 본문을 인덱스에도 들고 있었다. 검색 경로는 Supabase 에서
            # hydrate 하므로 본문 사본은 불필요하지만, 인덱스만으로 재구성이 가능하도록
            # 짧은 텍스트는 유지한다. (on_disk_payload 라 RAM 부담 없음)
            if document:
                payload.setdefault("document", document)
            points.append(
                models.PointStruct(id=point_id, vector=list(vector), payload=payload)
            )

        if points:
            self.client.upsert(
                collection_name=self.collection_name, points=points, wait=wait
            )

    def delete_where(self, where: Dict[str, Any]) -> None:
        from qdrant_client import models

        qfilter = _to_qdrant_filter(where)
        if qfilter is None:
            return
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=models.FilterSelector(filter=qfilter),
            wait=True,
        )

    def delete_ids(self, ids: Sequence[str]) -> None:
        from qdrant_client import models

        point_ids = [_to_point_id(i) for i in ids]
        if not point_ids:
            return
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=models.PointIdsList(points=point_ids),
            wait=True,
        )

    # ── 읽기 ──

    def query(self, embedding, top_k, where=None, with_metadata=False):
        from qdrant_client import models

        search_params = models.SearchParams(
            hnsw_ef=self.hnsw_ef,
            quantization=models.QuantizationSearchParams(
                ignore=False,
                # 양자화본으로 후보를 넓게 뽑고 디스크의 원본 벡터로 다시 채점한다.
                # 이게 없으면 int8 오차가 그대로 순위에 남는다.
                rescore=True,
                oversampling=self.oversampling,
            ),
        )
        result = self.client.query_points(
            collection_name=self.collection_name,
            query=list(embedding),
            limit=max(1, int(top_k)),
            query_filter=_to_qdrant_filter(where),
            search_params=search_params,
            with_payload=True if with_metadata else [_RAW_ID_KEY],
            with_vectors=False,
        )

        hits: List[Dict[str, Any]] = []
        for point in result.points:
            payload = point.payload or {}
            hits.append(
                {
                    "id": str(payload.get(_RAW_ID_KEY) or point.id),
                    "metadata": payload if with_metadata else None,
                }
            )
        return hits

    def get_embeddings(self, ids: Sequence[str]) -> Dict[str, List[float]]:
        point_ids = [_to_point_id(i) for i in ids]
        if not point_ids:
            return {}
        records = self.client.retrieve(
            collection_name=self.collection_name,
            ids=point_ids,
            with_vectors=True,
            with_payload=[_RAW_ID_KEY],
        )
        out: Dict[str, List[float]] = {}
        for rec in records:
            vector = rec.vector
            if vector is None:
                continue
            if isinstance(vector, dict):  # named vectors — 이 컬렉션은 단일 벡터라 미사용
                continue
            payload = rec.payload or {}
            out[str(payload.get(_RAW_ID_KEY) or rec.id)] = list(vector)
        return out

    def count(self) -> int:
        return int(
            self.client.count(collection_name=self.collection_name, exact=True).count
        )


def _to_qdrant_filter(where: Optional[Dict[str, Any]]):
    """Chroma where 표현식 → Qdrant ``Filter``. 비거나 번역 결과가 없으면 None."""
    from qdrant_client import models

    if not where:
        return None
    must, must_not = _parse_clause(where)
    if not must and not must_not:
        return None
    return models.Filter(must=must or None, must_not=must_not or None)


def _parse_clause(clause: Dict[str, Any]):
    """where 절 하나를 (must, must_not) 조건 리스트로 분해."""
    from qdrant_client import models

    must: List[Any] = []
    must_not: List[Any] = []

    for key, value in (clause or {}).items():
        if key == "$and":
            for sub in value or []:
                sub_must, sub_must_not = _parse_clause(sub)
                must.extend(sub_must)
                must_not.extend(sub_must_not)
            continue

        if key == "$or":
            # Qdrant 의 should 는 같은 Filter 안에 must 가 섞이면 의미가 흐려지므로
            # 중첩 Filter 로 감싸 OR 를 명시한다.
            branches: List[Any] = []
            for sub in value or []:
                sub_must, sub_must_not = _parse_clause(sub)
                branches.append(
                    models.Filter(must=sub_must or None, must_not=sub_must_not or None)
                )
            if branches:
                must.append(models.Filter(should=branches))
            continue

        if isinstance(value, dict):
            for op, operand in value.items():
                if op in ("$in",):
                    must.append(
                        models.FieldCondition(
                            key=key, match=models.MatchAny(any=list(operand))
                        )
                    )
                elif op in ("$nin",):
                    must_not.append(
                        models.FieldCondition(
                            key=key, match=models.MatchAny(any=list(operand))
                        )
                    )
                elif op in ("$eq",):
                    must.append(
                        models.FieldCondition(
                            key=key, match=models.MatchValue(value=operand)
                        )
                    )
                elif op in ("$ne",):
                    must_not.append(
                        models.FieldCondition(
                            key=key, match=models.MatchValue(value=operand)
                        )
                    )
                else:
                    raise ValueError(f"지원하지 않는 필터 연산자: {op} (key={key})")
            continue

        if isinstance(value, (list, tuple, set)):
            must.append(
                models.FieldCondition(key=key, match=models.MatchAny(any=list(value)))
            )
            continue

        must.append(models.FieldCondition(key=key, match=models.MatchValue(value=value)))

    return must, must_not


# ──────────────────────────────── factory ────────────────────────────────


def make_vector_index() -> VectorIndex:
    backend = config.vector_backend()
    if backend == "qdrant":
        return QdrantIndex()
    if backend == "chroma":
        return ChromaIndex()
    raise ValueError(f"Unknown VECTOR_BACKEND: {backend!r} (chroma | qdrant)")
