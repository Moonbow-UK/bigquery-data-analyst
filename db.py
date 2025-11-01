"""Compat wrapper around the persistence service implementation."""

from bqtools.services.persistence import (  # noqa: F401
    Base,
    Dataset,
    PersistenceService,
    SummaryExport,
    SummaryJob,
    SummaryReport,
)

persistence_service = PersistenceService()

__all__ = [
    "Base",
    "Dataset",
    "PersistenceService",
    "SummaryExport",
    "SummaryJob",
    "SummaryReport",
    "persistence_service",
]
