from __future__ import annotations

"""
Reusable helpers and service classes for interacting with BigQuery.

This package centralises authentication, client construction, and operations
so the CLI utilities, web UI, and future integrations can share the same code.
"""

from .analysis import (
    build_csv_summary,
    generate_ga4_intraday_summary,
    render_csv_summary,
    summarize_csv_file,
)
from .auth import DEFAULT_SCOPES, build_client, derive_project_id, load_credentials

# Optional persistence adapters (require SQLAlchemy or vector client libs).
try:  # pragma: no cover - optional dependency gate
    from .persistence import (
        SqlPersistenceAdapter,
        SqlPersistenceOptions,
        TableBatch,
        VectorStoreAdapter,
        VectorStoreClientProtocol,
        VectorStoreRecord,
        Vectoriser,
    )
except ModuleNotFoundError:  # pragma: no cover - dependency not installed
    SqlPersistenceAdapter = None  # type: ignore[assignment]
    SqlPersistenceOptions = None  # type: ignore[assignment]
    TableBatch = None  # type: ignore[assignment]
    VectorStoreAdapter = None  # type: ignore[assignment]
    VectorStoreClientProtocol = None  # type: ignore[assignment]
    VectorStoreRecord = None  # type: ignore[assignment]
    Vectoriser = None  # type: ignore[assignment]

from .services.bigquery_service import BigQueryService
from .services.dataset_summary import (
    DatasetSummaryOptions,
    DatasetSummaryService,
    IntradayTableNotFound,
)

__all__ = [
    "DEFAULT_SCOPES",
    "BigQueryService",
    "build_csv_summary",
    "generate_ga4_intraday_summary",
    "render_csv_summary",
    "summarize_csv_file",
    "DatasetSummaryOptions",
    "DatasetSummaryService",
    "IntradayTableNotFound",
    "build_client",
    "derive_project_id",
    "load_credentials",
]

if SqlPersistenceAdapter is not None:
    __all__.extend(
        [
            "SqlPersistenceAdapter",
            "SqlPersistenceOptions",
            "TableBatch",
            "VectorStoreAdapter",
            "VectorStoreClientProtocol",
            "VectorStoreRecord",
            "Vectoriser",
        ]
    )
