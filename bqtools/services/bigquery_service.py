from __future__ import annotations

import csv
import itertools
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Iterable, Optional

from google.cloud import bigquery

try:  # pragma: no cover - optional dependency
    from google.cloud import bigquery_storage_v1
except ImportError:  # pragma: no cover
    bigquery_storage_v1 = None

from ..storage import download_export_artifact, export_blob_uri, sync_export_artifact

logger = logging.getLogger(__name__)

from ..persistence.base import TableBatch

_TRUE_VALUES = {"1", "true", "yes", "on"}


class BigQueryService:
    """Encapsulates common BigQuery operations used across CLIs and APIs."""

    def __init__(self, client: bigquery.Client) -> None:
        self._client = client
        self._bqstorage_client: Optional["bigquery_storage_v1.BigQueryReadClient"] = None
        self._export_mode = os.environ.get("BIGQUERY_EXPORT_MODE", "gcs_extract").strip().lower()
        fallback_flag = os.environ.get("BIGQUERY_EXPORT_MODE_FALLBACK", "").strip().lower()
        self._allow_export_fallback = fallback_flag in _TRUE_VALUES

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
        supported_strategies = ("gcs_extract", "storage_api", "query_api")
        preferred = self._export_mode if self._export_mode in supported_strategies else "query_api"
        strategy_order: list[str] = [preferred]
        if self._allow_export_fallback:
            for candidate in supported_strategies:
                if candidate not in strategy_order:
                    strategy_order.append(candidate)

        last_error: Exception | None = None
        for strategy in strategy_order:
            strategy_timer = time.perf_counter()
            try:
                logger.info(
                    "Starting %s export for %s (destination=%s)",
                    strategy,
                    full_table_id,
                    output_path,
                )
                if strategy == "gcs_extract":
                    rows = self._export_via_gcs_extract(
                        full_table_id=full_table_id,
                        location=location,
                        output_path=output_path,
                    )
                if strategy == "storage_api":
                    rows = self._export_via_storage_api(
                        full_table_id=full_table_id,
                        location=location,
                        output_path=output_path,
                    )
                if strategy == "query_api":
                    rows = self._export_via_query_api(
                        full_table_id=full_table_id,
                        location=location,
                        output_path=output_path,
                    )
                elapsed = time.perf_counter() - strategy_timer
                logger.info(
                    "Completed %s export for %s in %.2fs (%s rows)",
                    strategy,
                    full_table_id,
                    elapsed,
                    rows,
                )
                return rows
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                elapsed = time.perf_counter() - strategy_timer
                logger.warning(
                    "Export strategy %s failed for %s after %.2fs: %s",
                    strategy,
                    full_table_id,
                    elapsed,
                    exc,
                )
                continue

        if last_error is not None:
            raise last_error

        raise RuntimeError("Failed to export table; no valid strategy executed.")

    def _export_via_query_api(
        self,
        *,
        full_table_id: str,
        location: str | None,
        output_path: Path,
    ) -> int:
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
        sync_export_artifact(output_path)
        return row_count

    def _export_via_gcs_extract(
        self,
        *,
        full_table_id: str,
        location: str | None,
        output_path: Path,
    ) -> int:
        destination_uri = export_blob_uri(output_path)
        if destination_uri is None:
            raise RuntimeError(
                "GCS extract export mode requires GCS_APP_BUCKET/APP_STORAGE_BUCKET to be configured."
            )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink()

        extract_config = bigquery.job.ExtractJobConfig(destination_format="CSV", print_header=True)
        job = self._client.extract_table(
            full_table_id,
            destination_uri,
            location=location,
            job_config=extract_config,
        )
        job.result()

        if not download_export_artifact(output_path):
            raise RuntimeError(
                f"Extract job succeeded but failed to download {destination_uri} to {output_path}."
            )
        row_count = self._count_csv_rows(output_path)
        sync_export_artifact(output_path)
        return row_count

    def _export_via_storage_api(
        self,
        *,
        full_table_id: str,
        location: str | None,  # unused but kept for parity
        output_path: Path,
    ) -> int:
        if bigquery_storage_v1 is None:
            raise RuntimeError(
                "Storage API export mode requires the google-cloud-bigquery-storage dependency."
            )
        client = self._get_bqstorage_client()
        if client is None:
            raise RuntimeError("Failed to initialise BigQuery Storage API client.")

        table = self._client.get_table(full_table_id)
        headers = [field.name for field in table.schema]

        project_id, dataset_id, table_name = full_table_id.split(".")
        table_resource = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_name}"
        read_session = bigquery_storage_v1.types.ReadSession(
            table=table_resource,
            data_format=bigquery_storage_v1.types.DataFormat.ARROW,
        )
        parent = f"projects/{self.project}"
        session = client.create_read_session(
            parent=parent,
            read_session=read_session,
            max_stream_count=1,
        )
        if not session.streams:
            return 0
        stream_name = session.streams[0].name
        reader = client.read_rows(stream_name)
        rows = reader.rows(session)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        row_count = 0
        with output_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(headers)
            for row in rows:
                if hasattr(row, "get"):
                    values = [row.get(field) for field in headers]
                else:
                    values = [row[field] for field in headers]
                writer.writerow(values)
                row_count += 1

        sync_export_artifact(output_path)
        return row_count

    def _get_bqstorage_client(self) -> Optional["bigquery_storage_v1.BigQueryReadClient"]:
        if bigquery_storage_v1 is None:
            return None
        if self._bqstorage_client is None:
            self._bqstorage_client = bigquery_storage_v1.BigQueryReadClient()
        return self._bqstorage_client

    @staticmethod
    def _count_csv_rows(path: Path) -> int:
        row_count = 0
        with path.open("r", newline="", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            header_seen = False
            for _ in reader:
                if not header_seen:
                    header_seen = True
                    continue
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
