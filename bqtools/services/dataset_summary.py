from __future__ import annotations

import os
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from google.api_core import exceptions as gcloud_exceptions
from google.cloud import bigquery


NUMERIC_TYPES = {"INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"}
DATE_TYPES = {"DATE", "DATETIME", "TIME", "TIMESTAMP"}
CATEGORICAL_TYPES = {"STRING", "BOOL"}

try:
    BaseCloudError = gcloud_exceptions.GoogleCloudError  # type: ignore[attr-defined]
except AttributeError:  # pragma: no cover
    BaseCloudError = gcloud_exceptions.GoogleAPIError


class IntradayTableNotFound(RuntimeError):
    """Raised when the expected intraday table is missing."""


@dataclass
class ColumnStat:
    column: str
    min_value: str | None = None
    max_value: str | None = None
    avg_value: str | None = None
    null_count: int | None = None
    total_rows: int | None = None

    def null_ratio(self) -> float | None:
        if self.null_count is None or self.total_rows in (None, 0):
            return None
        return self.null_count / self.total_rows


@dataclass
class TopValue:
    value: str
    count: int
    ratio: float | None = None


@dataclass
class DatasetSummaryOptions:
    location: str | None = None
    max_numeric_columns: int = 6
    max_categorical_columns: int = 4
    max_top_values: int = 5


class DatasetSummaryService:
    """Generate human-readable and structured summaries for BigQuery datasets or CSV exports."""

    def __init__(self, client: Optional[bigquery.Client]) -> None:
        self._client = client
        self._csv_mode = _env_flag("BIGQUERY_USE_CSV")
        csv_path_value = os.environ.get("BIGQUERY_USE_CSV_FILE")
        self._csv_path = Path(csv_path_value).expanduser() if csv_path_value else None

        if self._csv_mode:
            if self._csv_path is None:
                raise FileNotFoundError(
                    "CSV mode enabled (BIGQUERY_USE_CSV) but BIGQUERY_USE_CSV_FILE was not provided."
                )
            if not self._csv_path.exists():
                raise FileNotFoundError(
                    f"CSV mode enabled but CSV file not found at {self._csv_path}"
                )
        elif client is None:
            raise ValueError("BigQuery client is required when CSV mode is disabled.")

        self.csv_mode = self._csv_mode
        self.csv_path = self._csv_path

    def resolve_dataset_ref(self, dataset_id: str) -> bigquery.DatasetReference:
        if self._csv_mode:
            raise RuntimeError("resolve_dataset_ref is unavailable while BIGQUERY_USE_CSV is enabled.")
        cleaned = dataset_id.strip().strip("`")
        if "." in cleaned:
            project, dataset = cleaned.split(".", 1)
        elif ":" in cleaned:
            project, dataset = cleaned.split(":", 1)
        else:
            project, dataset = self._client.project, cleaned
        return bigquery.DatasetReference(project, dataset)

    def get_tables_for_summary(
        self,
        *,
        dataset_ref: bigquery.DatasetReference,
        all_tables: bool,
        intraday_date: str | None,
        intraday_table_prefix: str,
    ) -> tuple[list[bigquery.table.Table], str | None]:
        if self._csv_mode:
            note = f"CSV mode active: summarising {self._csv_path.name if self._csv_path else 'local CSV file'}."
            return [], note

        tables = list(self._client.list_tables(dataset_ref))
        filter_note: str | None = None

        if all_tables:
            return [self._client.get_table(t.reference) for t in tables], filter_note

        if intraday_date is None:
            intraday_date = datetime.now(timezone.utc).strftime("%Y%m%d")

        if not re.fullmatch(r"\d{8}", intraday_date):
            raise ValueError("--intraday-date must be in YYYYMMDD format.")

        expected_name = f"{intraday_table_prefix}{intraday_date}"
        filtered_items = [t for t in tables if t.table_id == expected_name]
        if not filtered_items:
            raise IntradayTableNotFound(
                "Intraday filter active but no table matched "
                f"`{expected_name}` in dataset "
                f"{dataset_ref.project}.{dataset_ref.dataset_id}."
            )

        filter_note = f"Intraday filter active: summarising `{expected_name}` only."
        resolved_tables = [self._client.get_table(item.reference) for item in filtered_items]
        return resolved_tables, filter_note

    def build_summary(
        self,
        *,
        dataset: Optional[bigquery.dataset.Dataset],
        tables: Iterable[bigquery.table.Table],
        options: DatasetSummaryOptions,
    ) -> dict[str, Any]:
        if self._csv_mode:
            return self._build_csv_summary(options)

        table_summaries: list[dict[str, Any]] = []
        table_overview: list[dict[str, Any]] = []

        for table_meta in tables:
            summary = self._collect_table_summary(
                table=table_meta,
                location=options.location,
                max_numeric=options.max_numeric_columns,
                max_categorical=options.max_categorical_columns,
                max_top_values=options.max_top_values,
            )
            table_summaries.append(summary)
            table_overview.append(
                {
                    "table_id": summary["table_id"],
                    "table_type": summary["table_type"],
                    "num_rows": summary["num_rows"],
                    "num_bytes": summary["num_bytes"],
                    "modified": summary["modified"],
                    "error": summary.get("error"),
                }
            )

        dataset_info = {
            "project": dataset.project,
            "dataset_id": dataset.dataset_id,
            "location": dataset.location,
            "description": dataset.description,
            "default_table_expiration_ms": dataset.default_table_expiration_ms,
            "default_partition_expiration_ms": dataset.default_partition_expiration_ms,
            "labels": dataset.labels,
            "table_count": len(table_overview),
        }

        return {
            "dataset": dataset_info,
            "table_overview": table_overview,
            "tables": table_summaries,
        }

    def summarise_to_text(
        self,
        *,
        summary: dict[str, Any],
    ) -> str:
        if summary.get("mode") == "csv":
            return summary.get("csv_report_text", "")

        dataset_info = summary["dataset"]
        dataset_block = "\n".join(
            [
                f"Dataset: {dataset_info['project']}.{dataset_info['dataset_id']}",
                f"Location: {dataset_info['location'] or 'unspecified'}",
                f"Default table expiration: {dataset_info['default_table_expiration_ms'] or 'not set'} ms",
                f"Default partition expiration: {dataset_info['default_partition_expiration_ms'] or 'not set'} ms",
                f"Labels: {dataset_info['labels'] or '{}'}",
                f"Description: {dataset_info['description'] or 'No dataset description provided.'}",
                f"Tables found: {dataset_info['table_count']}",
            ]
        )

        overview_rows = summary["table_overview"]
        overview_rows_formatted = []
        for row in overview_rows:
            if row.get("error"):
                overview_rows_formatted.append(
                    [
                        row["table_id"],
                        row["table_type"],
                        "n/a",
                        "n/a",
                        f"Failed to load table metadata ({row['error']})",
                    ]
                )
                continue
            overview_rows_formatted.append(
                [
                    row["table_id"],
                    row["table_type"],
                    "n/a" if row["num_rows"] is None else format_number(row["num_rows"]),
                    "n/a" if row["num_bytes"] is None else format_bytes(row["num_bytes"]),
                    "n/a" if row["modified"] is None else format_datetime(row["modified"]),
                ]
            )

        overview_block = render_table(
            headers=["Table", "Type", "Rows", "Storage", "Last modified"],
            rows=overview_rows_formatted or [["(none)", "—", "—", "—", "—"]],
        )

        table_details = [
            render_table_summary(table_summary)
            for table_summary in summary["tables"]
        ]

        return "\n\n".join(
            [
                dataset_block,
                "Table overview:",
                overview_block,
                "\n\n".join(table_details),
            ]
        )

    def _build_csv_summary(self, options: DatasetSummaryOptions) -> dict[str, Any]:
        from bqtools.analysis.ga4_intraday import generate_ga4_intraday_summary

        csv_summary, report_text = generate_ga4_intraday_summary(self._csv_path, options)
        summary_dict = asdict(csv_summary)

        dataset_info = {
            "project": "local",
            "dataset_id": self._csv_path.stem,
            "location": "CSV",
            "description": f"GA4 intraday summary generated from {self._csv_path}",
            "default_table_expiration_ms": None,
            "default_partition_expiration_ms": None,
            "labels": {"csv_mode": "true"},
            "table_count": 0,
        }

        overview_rows: list[dict[str, Any]] = []
        table_details: list[dict[str, Any]] = []
        csv_sections: list[dict[str, Any]] = []

        def add_section(name: str, rows: list[dict[str, str | None]], total_count: int) -> None:
            dataset_info["table_count"] += 1
            csv_sections.append(
                {
                    "title": name,
                    "rows": rows,
                    "include_share": any(bool(row.get("share")) for row in rows),
                }
            )
            overview_rows.append(
                {
                    "table_id": name,
                    "table_type": "CSV summary",
                    "num_rows": total_count,
                    "num_bytes": None,
                    "modified": None,
                    "error": None,
                }
            )
            table_details.append(
                {
                    "table_id": name,
                    "full_table_id": f"csv.{name.replace(' ', '_').lower()}",
                    "table_type": "CSV summary",
                    "num_rows": total_count,
                    "num_bytes": None,
                    "modified": None,
                    "description": f"Insights for {name.lower()}",
                    "time_partitioning": None,
                    "range_partitioning": None,
                    "clustering_fields": None,
                    "schema": [],
                    "schema_lines": [],
                    "numeric_stats": [],
                    "numeric_stats_error": None,
                    "categorical_stats": [],
                }
            )

        avg_events_per_session = (
            csv_summary.total_events / csv_summary.unique_sessions if csv_summary.unique_sessions else 0
        )
        avg_engagement_seconds = (
            (csv_summary.total_engagement_ms / 1000) / csv_summary.unique_sessions
            if csv_summary.unique_sessions
            else 0
        )
        engaged_rate = (
            csv_summary.engaged_sessions / csv_summary.unique_sessions if csv_summary.unique_sessions else 0
        )

        headline_rows = [
            {"label": "Total events", "value": f"{csv_summary.total_events:,}", "share": None},
            {"label": "Unique users", "value": f"{csv_summary.unique_users:,}", "share": None},
            {"label": "Unique sessions", "value": f"{csv_summary.unique_sessions:,}", "share": None},
            {
                "label": "Engaged sessions",
                "value": f"{csv_summary.engaged_sessions:,}",
                "share": f"{engaged_rate:.1%}",
            },
            {
                "label": "Avg. events per session",
                "value": f"{avg_events_per_session:.1f}",
                "share": None,
            },
            {
                "label": "Avg. engagement per session",
                "value": f"{avg_engagement_seconds:.1f} sec",
                "share": None,
            },
        ]
        if csv_summary.total_revenue:
            headline_rows.append(
                {
                    "label": "Revenue (USD)",
                    "value": f"${csv_summary.total_revenue:,.2f}",
                    "share": None,
                }
            )
        headline_rows.append(
            {
                "label": "Conversions",
                "value": f"{csv_summary.conversions:,}",
                "share": (
                    f"{csv_summary.conversion_rate:.1%}"
                    if csv_summary.conversion_rate is not None
                    else None
                ),
            }
        )
        add_section("Key metrics", headline_rows, csv_summary.total_events)

        def counter_rows(title: str, counter: Counter[str], base: Optional[int], limit: int) -> None:
            if not counter:
                return
            rows: list[dict[str, str | None]] = []
            total = sum(counter.values())
            denominator = base or total or 1
            for label, count in counter.most_common(limit):
                share = count / denominator if denominator else 0
                rows.append(
                    {
                        "label": label,
                        "value": f"{count:,}",
                        "share": f"{share:.1%}",
                    }
                )
            add_section(title, rows, total)

        limit = options.max_categorical_columns or 5
        counter_rows("Top events", csv_summary.events_counter, csv_summary.total_events, limit)
        counter_rows("Top pages", csv_summary.page_titles, csv_summary.total_events, limit)
        counter_rows("Device mix", csv_summary.device_categories, None, limit)
        counter_rows("Top countries", csv_summary.countries, None, limit)
        counter_rows("Login state", csv_summary.login_status, None, limit)

        quality_notes: list[dict[str, str | None]] = []
        if csv_summary.conversion_rate is None:
            reason = (
                "Missing session identifiers"
                if csv_summary.unique_sessions
                else "No sessions recorded"
            )
            quality_notes.append(
                {
                    "label": "Conversion rate",
                    "value": "n/a",
                    "share": reason,
                }
            )
        if csv_summary.conversions == 0:
            quality_notes.append(
                {
                    "label": "Conversions",
                    "value": "0",
                    "share": "No purchase events captured",
                }
            )
        if quality_notes:
            add_section("Data quality notes", quality_notes, len(quality_notes))

        return {
            "dataset": dataset_info,
            "table_overview": overview_rows,
            "tables": table_details,
            "mode": "csv",
            "csv_report_text": report_text,
            "ga4_summary": summary_dict,
            "csv_sections": csv_sections,
        }
    def _collect_table_summary(
        self,
        *,
        table: bigquery.table.Table,
        location: str | None,
        max_numeric: int,
        max_categorical: int,
        max_top_values: int,
    ) -> dict[str, Any]:
        numeric_columns, categorical_columns = classify_columns(table.schema)
        numeric_columns = numeric_columns[: max(0, max_numeric)]
        categorical_columns = categorical_columns[: max(0, max_categorical)]

        numeric_stats_records: list[dict[str, Any]] = []
        numeric_error: str | None = None
        try:
            numeric_stats = fetch_numeric_stats(
                client=self._client,
                table=table,
                columns=numeric_columns,
                location=location,
            )
            for stat in numeric_stats:
                numeric_stats_records.append(
                    {
                        "column": stat.column,
                        "min_value": stat.min_value,
                        "max_value": stat.max_value,
                        "avg_value": stat.avg_value,
                        "null_count": stat.null_count,
                        "null_ratio": stat.null_ratio(),
                    }
                )
        except BaseCloudError as exc:
            numeric_error = str(exc)

        categorical_stats: list[dict[str, Any]] = []
        for field in categorical_columns:
            try:
                top_values = fetch_top_values(
                    client=self._client,
                    table=table,
                    field=field,
                    max_values=max_top_values,
                    location=location,
                )
                categorical_stats.append(
                    {
                        "field": field.name,
                        "top_values": [
                            {
                                "value": tv.value,
                                "count": tv.count,
                                "ratio": tv.ratio,
                            }
                            for tv in top_values
                        ],
                        "error": None,
                    }
                )
            except BaseCloudError as exc:
                categorical_stats.append(
                    {
                        "field": field.name,
                        "top_values": [],
                        "error": str(exc),
                    }
                )

        time_partitioning = None
        if table.time_partitioning:
            tp = table.time_partitioning
            time_partitioning = {
                "type": tp.type_,
                "field": tp.field,
                "expiration_ms": tp.expiration_ms,
            }

        range_partitioning = None
        if table.range_partitioning:
            rp = table.range_partitioning
            range_partitioning = {
                "field": rp.field,
                "start": rp.range_.start,
                "end": rp.range_.end,
                "interval": rp.range_.interval,
            }

        return {
            "table_id": table.table_id,
            "full_table_id": table.full_table_id.replace(":", "."),
            "table_type": table.table_type,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "modified": table.modified,
            "description": table.description,
            "time_partitioning": time_partitioning,
            "range_partitioning": range_partitioning,
            "clustering_fields": table.clustering_fields,
            "schema": extract_schema_metadata(table),
            "schema_lines": describe_schema(table),
            "numeric_stats": numeric_stats_records,
            "numeric_stats_error": numeric_error,
            "categorical_stats": categorical_stats,
        }


def render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _join(values: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(values))

    separator = "-+-".join("-" * width for width in widths)
    lines = [_join(headers), separator]
    lines.extend(_join(row) for row in rows)
    return "\n".join(lines)


def describe_schema(table: bigquery.table.Table) -> list[str]:
    lines: list[str] = []
    for field in table.schema:
        mode = "array" if field.mode == "REPEATED" else field.mode.lower()
        description = f" – {field.description}" if field.description else ""
        lines.append(f"  - {field.name} ({field.field_type}, {mode}){description}")
    return lines


def extract_schema_metadata(table: bigquery.table.Table) -> list[dict[str, str | bool | None]]:
    metadata: list[dict[str, str | bool | None]] = []
    for field in table.schema:
        metadata.append(
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description,
                "is_repeated": field.mode == "REPEATED",
            }
        )
    return metadata


def classify_columns(
    schema: Sequence[bigquery.schema.SchemaField],
) -> tuple[list[bigquery.schema.SchemaField], list[bigquery.schema.SchemaField]]:
    numeric_candidates: list[bigquery.schema.SchemaField] = []
    categorical_candidates: list[bigquery.schema.SchemaField] = []
    for field in schema:
        if field.mode == "REPEATED":
            continue
        if field.field_type in NUMERIC_TYPES or field.field_type in DATE_TYPES:
            numeric_candidates.append(field)
        elif field.field_type in CATEGORICAL_TYPES:
            categorical_candidates.append(field)
    return numeric_candidates, categorical_candidates


def fetch_numeric_stats(
    *,
    client: bigquery.Client,
    table: bigquery.table.Table,
    columns: Sequence[bigquery.schema.SchemaField],
    location: str | None,
) -> list[ColumnStat]:
    if not columns:
        return []

    qualified_table = f"{table.project}.{table.dataset_id}.{table.table_id}"
    select_clauses = ["COUNT(1) AS total_rows"]
    alias_map: dict[tuple[str, str], str] = {}
    used_aliases: set[str] = set()

    for field in columns:
        if field.mode == "REPEATED":
            continue
        field_name = field.name
        alias_map[(field_name, "min")] = sanitise_alias(field_name, "min", used_aliases)
        alias_map[(field_name, "max")] = sanitise_alias(field_name, "max", used_aliases)
        alias_map[(field_name, "avg")] = sanitise_alias(field_name, "avg", used_aliases)
        alias_map[(field_name, "nulls")] = sanitise_alias(field_name, "nulls", used_aliases)

        select_clauses.append(f"MIN(`{field_name}`) AS `{alias_map[(field_name, 'min')]}`")
        select_clauses.append(f"MAX(`{field_name}`) AS `{alias_map[(field_name, 'max')]}`")
        if field.field_type in NUMERIC_TYPES:
            select_clauses.append(
                f"AVG(SAFE_CAST(`{field_name}` AS FLOAT64)) AS `{alias_map[(field_name, 'avg')]}`"
            )
        else:
            select_clauses.append(f"NULL AS `{alias_map[(field_name, 'avg')]}`")
        select_clauses.append(
            f"COUNTIF(`{field_name}` IS NULL) AS `{alias_map[(field_name, 'nulls')]}`"
        )

    sql = f"SELECT {', '.join(select_clauses)} FROM `{qualified_table}`"

    job = client.query(sql, location=location)
    result = list(job.result())
    if not result:
        return []
    row = result[0]
    total_rows = row["total_rows"]
    stats: list[ColumnStat] = []
    for field in columns:
        if field.mode == "REPEATED":
            continue
        name = field.name
        min_value = row.get(alias_map[(name, "min")])
        max_value = row.get(alias_map[(name, "max")])
        avg_value = row.get(alias_map[(name, "avg")])
        null_count = row.get(alias_map[(name, "nulls")])
        stats.append(
            ColumnStat(
                column=name,
                min_value=None if min_value is None else str(min_value),
                max_value=None if max_value is None else str(max_value),
                avg_value=None if avg_value is None else f"{avg_value:.2f}",
                null_count=None if null_count is None else int(null_count),
                total_rows=None if total_rows is None else int(total_rows),
            )
        )
    return stats


def fetch_top_values(
    *,
    client: bigquery.Client,
    table: bigquery.table.Table,
    field: bigquery.schema.SchemaField,
    max_values: int,
    location: str | None,
) -> list[TopValue]:
    if field.mode == "REPEATED":
        return []

    qualified_table = f"{table.project}.{table.dataset_id}.{table.table_id}"
    sql = (
        "WITH base AS ("
        f"  SELECT `{field.name}` AS value FROM `{qualified_table}`"
        "), stats AS ("
        "  SELECT COUNT(1) AS total_rows, COUNTIF(value IS NULL) AS null_rows FROM base"
        "), top_values AS ("
        "  SELECT top_entry.value AS value, top_entry.count AS count"
        "  FROM (SELECT APPROX_TOP_COUNT(value, @limit) AS top_arr FROM base),"
        "       UNNEST(top_arr) AS top_entry"
        ") "
        "SELECT top_values.value AS value, top_values.count AS count, "
        "       stats.total_rows AS total_rows, stats.null_rows AS null_rows "
        "FROM top_values CROSS JOIN stats"
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", max_values),
        ]
    )

    job = client.query(sql, job_config=job_config, location=location)
    rows = list(job.result())

    results: list[TopValue] = []
    for row in rows:
        total_rows = row["total_rows"] or 0
        null_rows = row["null_rows"] or 0
        denominator = max(total_rows - null_rows, 1)
        ratio = (row["count"] or 0) / denominator if denominator else None
        value = row["value"]
        value_str = "NULL" if value is None else str(value)
        results.append(
            TopValue(
                value=value_str,
                count=int(row["count"]),
                ratio=ratio,
            )
        )
    return results


def sanitise_alias(column: str, suffix: str, existing: set[str]) -> str:
    base = re.sub(r"\W+", "_", column).strip("_") or "col"
    candidate = f"{base}_{suffix}".lower()
    if candidate[0].isdigit():
        candidate = f"f_{candidate}"
    while candidate in existing:
        candidate = f"{candidate}_"
    existing.add(candidate)
    return candidate


def format_number(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,}"


def format_float(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{decimals}f}"


def format_bytes(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "n/a"
    if num_bytes < 1024:
        return f"{num_bytes} B"

    units = ["KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    for unit in units:
        size /= 1024.0
        if size < 1024.0:
            return f"{size:,.1f} {unit}"
    return f"{size:,.1f} EB"


def format_datetime(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def render_table_summary(summary: dict[str, Any]) -> str:
    header_lines = [
        f"Table: {summary['full_table_id']}",
        f"Type: {summary['table_type']}",
        f"Rows: {format_number(summary['num_rows'])}",
        f"Stored data: {format_bytes(summary['num_bytes'])}",
        f"Last modified: {format_datetime(summary['modified'])}",
    ]
    if summary["description"]:
        header_lines.append(f"Description: {summary['description']}")
    if summary["time_partitioning"]:
        partition_info = summary["time_partitioning"]
        field = partition_info.get("field") or "ingestion_time"
        header_lines.append(
            f"Partitioned by {field} (type: {partition_info.get('type')}, field: {field})"
        )
    if summary["range_partitioning"]:
        rp = summary["range_partitioning"]
        header_lines.append(
            f"Range partitioned by {rp['field']} "
            f"[{rp['start']}, {rp['end']}) step {rp['interval']}"
        )
    if summary["clustering_fields"]:
        header_lines.append(f"Clustered by {', '.join(summary['clustering_fields'])}")

    schema_lines = summary.get("schema_lines") or []
    if not schema_lines:
        schema_lines = ["  (no schema available)"]

    stats_blocks: list[str] = []

    numeric_stats = summary["numeric_stats"]
    numeric_error = summary["numeric_stats_error"]
    if numeric_stats:
        rows = []
        for stat in numeric_stats:
            rows.append(
                [
                    stat["column"],
                    stat["min_value"] or "n/a",
                    stat["max_value"] or "n/a",
                    stat["avg_value"] or "n/a",
                    format_float(stat["null_ratio"]),
                ]
            )
        stats_blocks.append(
            "\n".join(
                [
                    "Numeric/date stats:",
                    render_table(
                        headers=["Column", "Min", "Max", "Avg", "Null ratio"],
                        rows=rows,
                    ),
                ]
            )
        )
    elif numeric_error:
        stats_blocks.append(f"Numeric/date stats unavailable: {numeric_error}")
    else:
        stats_blocks.append("Numeric/date stats: (none)")

    categorical_stats = summary["categorical_stats"]
    if categorical_stats:
        for group in categorical_stats:
            header = f"Top values for `{group['field']}`:"
            if group["error"]:
                stats_blocks.append(f"{header} unavailable ({group['error']})")
                continue
            rows = [
                [
                    value["value"],
                    f"{value['count']:,}",
                    format_float(value["ratio"]),
                ]
                for value in group["top_values"]
            ]
            stats_blocks.append(
                "\n".join(
                    [
                        header,
                        render_table(headers=["Value", "Count", "Ratio"], rows=rows),
                    ]
                )
            )
    else:
        stats_blocks.append("Categorical stats: (none)")

    return "\n".join(
        [
            "\n".join(header_lines),
            "",
            "Schema:",
            "\n".join(schema_lines),
            "",
            "\n\n".join(stats_blocks),
        ]
    )


def _env_flag(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}
