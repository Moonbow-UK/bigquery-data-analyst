from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence


@dataclass(frozen=True)
class TableBatch:
    """Container for tabular data returned by BigQueryService."""

    table_id: str
    headers: Sequence[str]
    rows: Sequence[Sequence[Any]]
    schema: Sequence[Any] | None = None  # Optional BigQuery SchemaField instances

    def as_dicts(self) -> list[dict[str, Any]]:
        """Convert rows to dictionaries keyed by header."""
        result: list[dict[str, Any]] = []
        for row in self.rows:
            result.append({header: row[idx] if idx < len(row) else None for idx, header in enumerate(self.headers)})
        return result

    def iter_dicts(self) -> Iterable[dict[str, Any]]:
        for row in self.rows:
            yield {header: row[idx] if idx < len(row) else None for idx, header in enumerate(self.headers)}
