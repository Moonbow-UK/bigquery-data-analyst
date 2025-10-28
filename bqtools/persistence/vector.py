from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from .base import TableBatch


class VectorStoreClientProtocol(Protocol):
    """Minimal protocol for vector databases (for example, pgvector, Pinecone, Chroma)."""

    def upsert(
        self,
        *,
        ids: Sequence[str],
        vectors: Sequence[Sequence[float]],
        metadatas: Sequence[dict[str, Any]] | None = None,
    ) -> Any:
        ...


@dataclass(slots=True)
class VectorStoreRecord:
    id: str
    vector: Sequence[float]
    metadata: dict[str, Any]


class Vectoriser(Protocol):
    """Protocol for callables that produce embeddings from row metadata."""

    def __call__(self, row: dict[str, Any]) -> Sequence[float]:
        ...


class VectorStoreAdapter:
    """
    Store BigQuery rows in a vector database by creating embeddings per row.

    Provide a client that matches `VectorStoreClientProtocol` (pgvector, Pinecone,
    Chroma, etc.) and a vectoriser callable that returns embeddings from a row.
    """

    def __init__(
        self,
        client: VectorStoreClientProtocol,
        *,
        vectoriser: Vectoriser,
        metadata_builder: callable[[dict[str, Any]], dict[str, Any]] | None = None,
        id_builder: callable[[dict[str, Any]], str] | None = None,
    ) -> None:
        self._client = client
        self._vectoriser = vectoriser
        self._metadata_builder = metadata_builder or (lambda row: row)
        self._id_builder = id_builder or (lambda row: str(uuid.uuid4()))

    def persist_batch(
        self,
        batch: TableBatch,
        *,
        namespace: str | None = None,
    ) -> Sequence[VectorStoreRecord]:
        records: list[VectorStoreRecord] = []
        for row_dict in batch.iter_dicts():
            vector = self._vectoriser(row_dict)
            if not vector:
                continue
            record = VectorStoreRecord(
                id=self._id_builder(row_dict),
                vector=vector,
                metadata=self._augment_metadata(batch.table_id, row_dict, namespace),
            )
            records.append(record)

        if not records:
            return records

        self._client.upsert(
            ids=[record.id for record in records],
            vectors=[record.vector for record in records],
            metadatas=[record.metadata for record in records],
        )
        return records

    def _augment_metadata(
        self,
        table_id: str,
        row: dict[str, Any],
        namespace: str | None,
    ) -> dict[str, Any]:
        base = dict(self._metadata_builder(row))
        base.setdefault("_source_table", table_id)
        if namespace:
            base.setdefault("_namespace", namespace)
        return base
