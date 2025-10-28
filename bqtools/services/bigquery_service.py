from __future__ import annotations

import csv
import itertools
import json
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from ..persistence.base import TableBatch


class BigQueryService:
    """Encapsulates common BigQuery operations used across CLIs and APIs."""

    def __init__(self, client: bigquery.Client) -> None:
        self._client = client

    @property
    def project(self) -> str:
        return self._client.project

    def run_test_query(self, query: str, *, location: str | None, max_rows: int) -> dict[str, Any]:
        job = self._client.query(query, location=location)
        result = job.result()
        rows = list(itertools.islice(result, max_rows))
        headers = tuple(rows[0].keys()) if rows else tuple()
        return {
            "headers": headers,
            "rows": [[row[field] for field in headers] for row in rows],
        }

    def list_tables(self, dataset_id: str | None = None) -> list[dict[str, str]]:
        if dataset_id:
            dataset_project, dataset_name = self.normalize_dataset_identifier(dataset_id)
            dataset_ref = bigquery.DatasetReference(dataset_project, dataset_name)
            tables = list(self._client.list_tables(dataset_ref))
            return [
                {
                    "project": table.project,
                    "dataset": table.dataset_id,
                    "table": table.table_id,
                }
                for table in tables
            ]

        datasets = list(self._client.list_datasets())
        listings: list[dict[str, str]] = []
        for dataset in datasets:
            tables = list(self._client.list_tables(dataset.reference))
            for table in tables:
                listings.append(
                    {
                        "project": table.project,
                        "dataset": table.dataset_id,
                        "table": table.table_id,
                    }
                )
        return listings

    def preview_table(
        self,
        table_id: str,
        *,
        location: str | None,
        max_rows: int,
    ) -> dict[str, Any]:
        batch = self.preview_table_batch(
            table_id=table_id,
            location=location,
            max_rows=max_rows,
        )
        return {
            "table_id": batch.table_id,
            "headers": tuple(batch.headers),
            "rows": [list(row) for row in batch.rows],
        }

    def preview_table_batch(
        self,
        table_id: str,
        *,
        location: str | None,
        max_rows: int,
    ) -> TableBatch:
        full_table_id = self.normalize_table_identifier(table_id)
        query = f"SELECT * FROM `{full_table_id}` LIMIT {max_rows}"
        job = self._client.query(query, location=location)
        result = job.result()
        schema_fields = result.schema
        headers = [field.name for field in schema_fields]
        rows = list(itertools.islice(result, max_rows))
        values = [[row[field] for field in headers] for row in rows]
        return TableBatch(
            table_id=full_table_id,
            headers=headers,
            rows=values,
            schema=schema_fields,
        )

    def fetch_row_count(
        self,
        table_id: str,
        *,
        location: str | None,
    ) -> int:
        full_table_id = self.normalize_table_identifier(table_id)
        query = f"SELECT COUNT(1) AS row_count FROM `{full_table_id}`"
        job = self._client.query(query, location=location)
        result = job.result()
        row = next(result, None)
        if row is None:
            raise RuntimeError(f"No rows returned while counting `{full_table_id}`.")
        count = row.get("row_count")
        if count is None:
            raise RuntimeError(f"Row count for `{full_table_id}` is unavailable.")
        return int(count)

    def export_table_to_csv(
        self,
        table_id: str,
        *,
        location: str | None,
        output_path: Path,
    ) -> int:
        full_table_id = self.normalize_table_identifier(table_id)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        query = f"SELECT * FROM `{full_table_id}`"
        job = self._client.query(query, location=location)
        result = job.result(page_size=1000)
        headers = [field.name for field in result.schema]

        row_count = 0
        with output_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(headers)
            for row in result:
                writer.writerow([self._coerce_csv_value(row[field]) for field in headers])
                row_count += 1
        return row_count

    def export_dataset_to_csv(
        self,
        dataset_id: str,
        *,
        location: str | None,
        output_dir: Path,
    ) -> list[Path]:
        dataset_project, dataset_name = self.normalize_dataset_identifier(dataset_id)
        dataset_ref = bigquery.DatasetReference(dataset_project, dataset_name)
        tables = list(self._client.list_tables(dataset_ref))
        output_dir.mkdir(parents=True, exist_ok=True)
        written_files: list[Path] = []
        for table in tables:
            full_table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"
            output_path = output_dir / f"{table.table_id}.csv"
            self.export_table_to_csv(
                full_table_id,
                location=location,
                output_path=output_path,
            )
            written_files.append(output_path)
        return written_files

    def normalize_table_identifier(self, table_id: str) -> str:
        cleaned = table_id.strip().strip("`")
        if cleaned.count(".") == 2:
            return cleaned
        if cleaned.count(".") == 1:
            return f"{self.project}.{cleaned}"
        raise RuntimeError(
            "Table identifier must be of the form DATASET.TABLE or PROJECT.DATASET.TABLE."
        )

    def normalize_dataset_identifier(self, dataset_id: str) -> tuple[str, str]:
        cleaned = dataset_id.strip().strip("`")
        if "." in cleaned:
            return tuple(cleaned.split(".", 1))  # type: ignore[return-value]
        return self.project, cleaned

    @staticmethod
    def _coerce_csv_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value
