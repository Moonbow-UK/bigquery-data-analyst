from __future__ import annotations

"""
Persistence adapters that can store BigQuery query results in downstream systems.
"""

from .base import TableBatch

try:  # pragma: no cover - optional dependency
    from .sql import SqlPersistenceAdapter, SqlPersistenceOptions
except ModuleNotFoundError:  # pragma: no cover
    SqlPersistenceAdapter = None  # type: ignore[assignment]
    SqlPersistenceOptions = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from .vector import VectorStoreAdapter, VectorStoreRecord, VectorStoreClientProtocol, Vectoriser
except ModuleNotFoundError:  # pragma: no cover
    VectorStoreAdapter = None  # type: ignore[assignment]
    VectorStoreRecord = None  # type: ignore[assignment]
    VectorStoreClientProtocol = None  # type: ignore[assignment]
    Vectoriser = None  # type: ignore[assignment]

__all__ = ["TableBatch"]

if SqlPersistenceAdapter is not None:
    __all__.extend(["SqlPersistenceAdapter", "SqlPersistenceOptions"])

if VectorStoreAdapter is not None:
    __all__.extend(
        [
            "VectorStoreAdapter",
            "VectorStoreClientProtocol",
            "VectorStoreRecord",
            "Vectoriser",
        ]
    )
