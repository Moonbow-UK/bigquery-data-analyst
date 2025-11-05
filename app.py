#!/usr/bin/env python3
from __future__ import annotations

import copy
import csv
import json
import logging
import os
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, DecimalException
from logging.handlers import BaseRotatingHandler
from pathlib import Path
from queue import SimpleQueue
from threading import Thread
from typing import Any, Callable, Sequence

from openai import OpenAI
from pinecone import Pinecone

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from google.api_core import exceptions as gcloud_exceptions
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload

from bqtools import (
    DEFAULT_SCOPES,
    BigQueryService,
    DatasetSummaryOptions,
    DatasetSummaryService,
    IntradayTableNotFound,
    build_client,
)
from bqtools.api import SummaryAPIConfig, create_summary_blueprint
from bqtools.config import default_credentials_file, load_environment
from bqtools.services.dataset_summary import BaseCloudError
from bqtools.services.persistence import (
    Dataset,
    PersistenceService,
    SummaryExport,
    SummaryJob,
    SummaryReport,
)

load_environment()

def _env_flag_from_str(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

DEBUG_MODE = _env_flag_from_str(os.environ.get("DEBUG_MODE"), False)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "change-me")
app.config["DEFAULT_CREDENTIALS_FILE"] = str(default_credentials_file())
app.config["DEBUG"] = DEBUG_MODE


summary_api = create_summary_blueprint(
    SummaryAPIConfig(default_credentials=default_credentials_file())
)
app.register_blueprint(summary_api, url_prefix="/api")

class DailyPrefixedFileHandler(BaseRotatingHandler):
    """Rotate log files daily with filenames like YYYY-MM-DD-app.log."""

    def __init__(
        self,
        directory: Path,
        base_name: str,
        *,
        prefix_format: str = "%Y-%m-%d",
        encoding: str | None = "utf-8",
        delay: bool = True,
        utc: bool = False,
    ) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.base_name = base_name
        self.prefix_format = prefix_format
        self.utc = utc
        self._current_date = None
        initial_date = self._now().date()
        initial_path = self._path_for_date(initial_date)
        super().__init__(str(initial_path), "a", encoding=encoding, delay=delay)
        self._current_date = initial_date

    def _now(self) -> datetime:
        return datetime.now(timezone.utc) if self.utc else datetime.now()

    def _path_for_date(self, current_date: date) -> Path:
        prefix = current_date.strftime(self.prefix_format)
        return self.directory / f"{prefix}-{self.base_name}"

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        record_dt = datetime.fromtimestamp(
            record.created,
            tz=timezone.utc if self.utc else None,
        )
        record_date = record_dt.date()
        if self._current_date != record_date:
            self._next_date = record_date
            return True
        return False

    def doRollover(self) -> None:
        if getattr(self, "stream", None):
            self.stream.close()
            self.stream = None
        self._current_date = getattr(self, "_next_date", self._now().date())
        new_path = self._path_for_date(self._current_date)
        self.baseFilename = str(new_path)
        self.stream = self._open()


LOG_DIR = Path("var/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

EXPORT_ROOT = Path("var/exports")
CSV_EXPORT_DIR = EXPORT_ROOT / "csv"
JSON_EXPORT_DIR = EXPORT_ROOT / "json"
for export_dir in (CSV_EXPORT_DIR, JSON_EXPORT_DIR):
    export_dir.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("summary_app")
if not logger.handlers:
    logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)
    file_handler = DailyPrefixedFileHandler(LOG_DIR, "app.log")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    )
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    )
logger.addHandler(stream_handler)
logger.propagate = False

if DEBUG_MODE:
    logger.debug("Debug mode enabled for summary_app.")

persistence_service = PersistenceService(logger=logger.getChild("persistence"))

CHAT_SUGGESTIONS: list[str] = [
    "Show me which product categories have the highest cart abandonment rate this week.",
    "Identify pages with the lowest conversion rates and suggest possible reasons.",
    "Where are we losing the most potential revenue across the customer journey?",
    "Summarise noteworthy GA4 trends from the past seven days.",
]

CHAT_SYSTEM_PROMPT = (
    "You are Tracey, a senior ecommerce analyst supporting the Euronics team. "
    "Use the supplied GA4 summary context to answer questions with clear, data-backed insights. "
    "Reference date ranges, call out significant changes, and propose next steps when appropriate. "
    "If the context is insufficient, explain what is missing rather than guessing. "
    "Respond as JSON with keys 'answer', 'highlights', and 'followups'. "
    "'answer' should be plain text (multiple paragraphs allowed). "
    "'highlights' must be an array of short bullet strings (can be empty). "
    "'followups' must be an array with up to three short questions the user could ask next."
)

_OPENAI_CLIENT: OpenAI | None = None
_PINECONE_CLIENT: Pinecone | None = None
_PINECONE_INDEX_CACHE: dict[str, Any] = {}

CHAT_MODEL = os.environ.get("OPENAI_CHAT_MODEL", "gpt-4o-mini")
EMBED_MODEL = os.environ.get("OPENAI_EMBED_MODEL", "text-embedding-3-small")
PINECONE_NAMESPACE = os.environ.get("PINECONE_NAMESPACE", "ga4")
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME")
PINECONE_ENVIRONMENT = os.environ.get("PINECONE_ENVIRONMENT")
CHAT_TOP_K = max(1, int(os.environ.get("AI_ASSISTANT_TOP_K", "4")))


def _csv_export_path(project_id: str, dataset_id: str, table_name: str) -> Path:
    filename = f"{project_id}_{dataset_id}_{table_name}.csv"
    legacy = Path(filename)
    target = CSV_EXPORT_DIR / filename
    if legacy.exists() and not target.exists():
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            legacy.replace(target)
            logger.info("Migrated legacy CSV %s to %s", legacy, target)
        except OSError as exc:
            logger.warning("Unable to move legacy CSV %s to %s: %s", legacy, target, exc)
    return target


def _json_export_path(project_id: str, dataset_id: str, table_name: str) -> Path:
    filename = f"{project_id}_{dataset_id}_{table_name}.json"
    legacy = Path(filename)
    target = JSON_EXPORT_DIR / filename
    if legacy.exists() and not target.exists():
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            legacy.replace(target)
            logger.info("Migrated legacy JSON %s to %s", legacy, target)
        except OSError as exc:
            logger.warning("Unable to move legacy JSON %s to %s: %s", legacy, target, exc)
    return target


def _purge_existing_summary_jobs(
    session: Session,
    dataset_record: Dataset,
    range_key: str,
    *,
    statuses: tuple[str, ...] | None = None,
    exclude_job_id: uuid.UUID | None = None,
) -> int:
    stmt = select(SummaryJob).where(
        SummaryJob.dataset_id == dataset_record.id,
        SummaryJob.range_key == range_key,
    )
    if statuses is not None:
        stmt = stmt.where(SummaryJob.status.in_(statuses))
    if exclude_job_id is not None:
        stmt = stmt.where(SummaryJob.id != exclude_job_id)
    jobs = session.execute(stmt).scalars().all()
    if not jobs:
        return 0
    for job in jobs:
        session.delete(job)
    session.commit()
    logger.info(
        "Removed %d existing summary job(s) for %s.%s range=%s",
        len(jobs),
        dataset_record.project_id,
        dataset_record.dataset_id,
        range_key,
    )
    return len(jobs)


def _coerce_positive_int(value: Any, default: int = 1) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float, Decimal)):
        try:
            coerced = int(value)
        except (OverflowError, ValueError):
            return default
        return coerced if coerced > 0 else default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            coerced = int(Decimal(text))
        except (ValueError, ArithmeticError, DecimalException):
            return default
        return coerced if coerced > 0 else default
    return default


def _compute_product_purchase_stats(csv_paths: list[Path], top_n: int = 10) -> dict[str, list[dict[str, object]]]:
    counts: Counter[str] = Counter()
    for csv_path in csv_paths:
        try:
            with csv_path.open("r", newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                if not reader.fieldnames:
                    continue
                for row in reader:
                    event_name = (row.get("event_name") or "").strip().lower()
                    if event_name != "purchase":
                        continue
                    items_payload = row.get("items")
                    if not items_payload:
                        continue
                    try:
                        parsed_items = json.loads(items_payload)
                    except json.JSONDecodeError:
                        logger.debug("Failed to parse items payload for %s", csv_path)
                        continue
                    if not isinstance(parsed_items, list):
                        continue
                    for item in parsed_items:
                        if not isinstance(item, dict):
                            continue
                        name = (
                            item.get("item_name")
                            or item.get("item_id")
                            or item.get("item_brand")
                            or "Unknown product"
                        )
                        name_str = str(name).strip() or "Unknown product"
                        quantity = _coerce_positive_int(item.get("quantity"), default=1)
                        counts[name_str] += quantity
        except FileNotFoundError:
            logger.debug("CSV path missing during product stats computation: %s", csv_path)
            continue
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unable to compute product stats from %s: %s", csv_path, exc)

    if not counts:
        return {"most_purchased": [], "least_purchased": []}

    most = [
        {"name": name, "count": count}
        for name, count in counts.most_common(top_n)
    ]

    least_sorted = sorted(counts.items(), key=lambda item: (item[1], item[0]))
    least = [{"name": name, "count": count} for name, count in least_sorted[:top_n]]
    return {"most_purchased": most, "least_purchased": least}

def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _week_range_days(default: int = 7) -> int:
    raw_value = os.environ.get("BIGQUERY_WEEK_RANGE_DAYS")
    if raw_value is None:
        return default
    raw_value = raw_value.strip()
    if not raw_value:
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        return default
    if parsed <= 0:
        return default
    return parsed


def _default_dataset() -> str:
    project = os.environ.get("BIGQUERY_PROJECT_ID", "euronics-1047")
    dataset_id = os.environ.get("BIGQUERY_DATASET_ID", "analytics_308868785")
    return f"{project}.{dataset_id}"


def _split_dataset(dataset_identifier: str) -> tuple[str, str]:
    if "." not in dataset_identifier:
        raise ValueError("Dataset identifier must include project and dataset (project.dataset)")
    project, dataset_id = dataset_identifier.split(".", 1)
    return project, dataset_id


def _table_name_for_range(range_key: str, *, reference_time: datetime) -> str:
    date_str = reference_time.strftime("%Y%m%d")
    if range_key == "today":
        return f"events_intraday_{date_str}"
    if range_key == "yesterday":
        return f"events_{date_str}"
    raise ValueError("Selected date range is not supported yet.")


def _build_client(credentials_file: str, scopes: tuple[str, ...], project_override: str | None):
    return build_client(
        credentials_file=Path(credentials_file).expanduser(),
        scopes=scopes,
        project_override=project_override,
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _format_elapsed_label(timestamp: datetime, *, reference: datetime | None = None) -> str:
    base = reference or _now_utc()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    else:
        base = base.astimezone(timezone.utc)

    seconds_total = int((base - timestamp).total_seconds())
    if seconds_total < 0:
        seconds_total = 0
    if seconds_total < 60:
        return f"{seconds_total}s ago"
    minutes, seconds = divmod(seconds_total, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s ago"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m ago"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h ago"


def _table_kind_from_name(table_name: str) -> str:
    lowered = table_name.lower()
    if "intraday" in lowered:
        return "intraday"
    if "events" in lowered:
        return "daily"
    return "unknown"


_INTRADAY_HINT_MESSAGE = (
    "Daily GA4 table not yet published; using intraday snapshot. Run the summary again once the final table is available."
)


def _is_not_found_error(error: BaseCloudError) -> bool:
    if isinstance(error, gcloud_exceptions.NotFound):
        return True
    code = getattr(error, "code", None)
    if code in (404, "404"):
        return True
    message = getattr(error, "message", "") or str(error)
    return "not found" in message.lower()


def _intraday_hint(summary: dict | None, fallback_flag: bool | None = None) -> str:
    is_intraday = False
    if summary and isinstance(summary, dict):
        is_intraday = bool(summary.get("intraday_active"))
    if fallback_flag is not None:
        is_intraday = is_intraday or bool(fallback_flag)
    return _INTRADAY_HINT_MESSAGE if is_intraday else ""


def _jsonify(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Counter):
        return {str(key): _jsonify(val) for key, val in value.items()}
    if isinstance(value, dict):
        return {str(key): _jsonify(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonify(item) for item in value]
    return str(value)


def _use_database_persistence() -> bool:
    return persistence_service.is_database_mode()


def _enter_csv_mode(summary_csv_path: Path) -> None:
    os.environ["BIGQUERY_USE_CSV"] = "true"
    os.environ["BIGQUERY_USE_CSV_FILE"] = str(summary_csv_path)


def _exit_csv_mode() -> None:
    os.environ.pop("BIGQUERY_USE_CSV", None)
    os.environ.pop("BIGQUERY_USE_CSV_FILE", None)


def _write_json_from_csv(csv_path: Path, json_path: Path) -> int:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with csv_path.open("r", newline="", encoding="utf-8") as csv_file, json_path.open(
        "w", encoding="utf-8"
    ) as json_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            json_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            row_count += 1
    return row_count


def _combine_csv_files(csv_paths: list[Path], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    header: list[str] | None = None
    writer = None
    total_rows = 0
    with output_path.open("w", newline="", encoding="utf-8") as out_file:
        for csv_path in csv_paths:
            with csv_path.open("r", newline="", encoding="utf-8") as in_file:
                reader = csv.reader(in_file)
                file_header = next(reader, None)
                if file_header is None:
                    continue
                if header is None:
                    header = file_header
                    writer = csv.writer(out_file)
                    writer.writerow(header)
                elif file_header != header:
                    raise ValueError(
                        "Header mismatch when combining CSV files; "
                        f"encountered differing schema in {csv_path.name}."
                    )
                if writer is None:
                    continue
                for row in reader:
                    writer.writerow(row)
                    total_rows += 1
    if header is None:
        try:
            output_path.unlink()
        except FileNotFoundError:
            pass
        raise ValueError("No data available to combine for the selected date range.")
    return total_rows


def _resolve_exports(
    range_key: str,
    *,
    now: datetime,
    project_id: str,
    dataset_id: str,
    week_days: int,
    intraday_prefix: str,
) -> tuple[list[dict[str, Path | str | datetime]], str, Path]:
    normalized_key = range_key.lower()
    if normalized_key == "today":
        target_date = now
        date_str = target_date.strftime("%Y%m%d")
        table_name = f"{intraday_prefix}{date_str}"
        csv_path = _csv_export_path(project_id, dataset_id, table_name)
        json_path = _json_export_path(project_id, dataset_id, table_name)
        exports = [
            {
                "table_name": table_name,
                "full_table_id": f"{project_id}.{dataset_id}.{table_name}",
                "csv_path": csv_path,
                "json_path": json_path,
                "date": target_date,
            }
        ]
        return exports, "Today", csv_path
    if normalized_key == "yesterday":
        target_date = now - timedelta(days=1)
        date_str = target_date.strftime("%Y%m%d")
        table_name = f"events_{date_str}"
        csv_path = _csv_export_path(project_id, dataset_id, table_name)
        json_path = _json_export_path(project_id, dataset_id, table_name)
        fallback_table_name = f"{intraday_prefix}{date_str}"
        fallback_csv_path = _csv_export_path(project_id, dataset_id, fallback_table_name)
        fallback_json_path = _json_export_path(project_id, dataset_id, fallback_table_name)
        exports = [
            {
                "table_name": table_name,
                "full_table_id": f"{project_id}.{dataset_id}.{table_name}",
                "csv_path": csv_path,
                "json_path": json_path,
                "date": target_date,
                "fallback_table_name": fallback_table_name,
                "fallback_full_table_id": f"{project_id}.{dataset_id}.{fallback_table_name}",
                "fallback_csv_path": fallback_csv_path,
                "fallback_json_path": fallback_json_path,
            }
        ]
        return exports, "Yesterday", csv_path
    if normalized_key == "last7days":
        exports: list[dict[str, Path | str | datetime]] = []
        days_to_fetch = max(1, week_days or 7)
        for offset in range(1, days_to_fetch + 1):
            target_date = now - timedelta(days=offset)
            date_str = target_date.strftime("%Y%m%d")
            table_name = f"events_{date_str}"
            csv_path = _csv_export_path(project_id, dataset_id, table_name)
            json_path = _json_export_path(project_id, dataset_id, table_name)
            fallback_table_name = f"{intraday_prefix}{date_str}"
            fallback_csv_path = _csv_export_path(project_id, dataset_id, fallback_table_name)
            fallback_json_path = _json_export_path(project_id, dataset_id, fallback_table_name)
            exports.append(
                {
                    "table_name": table_name,
                    "full_table_id": f"{project_id}.{dataset_id}.{table_name}",
                    "csv_path": csv_path,
                    "json_path": json_path,
                    "date": target_date,
                    "fallback_table_name": fallback_table_name,
                    "fallback_full_table_id": f"{project_id}.{dataset_id}.{fallback_table_name}",
                    "fallback_csv_path": fallback_csv_path,
                    "fallback_json_path": fallback_json_path,
                }
            )
        summary_csv = _csv_export_path(
            project_id,
            dataset_id,
            f"events_last{days_to_fetch}days",
        )
        day_label = "day" if days_to_fetch == 1 else "days"
        return exports, f"Last {days_to_fetch} {day_label}", summary_csv
    if normalized_key.startswith("date:"):
        _, _, date_part = normalized_key.partition(":")
        try:
            target_date = datetime.strptime(date_part, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Invalid date format. Use YYYY-MM-DD.") from exc
        if target_date.date() > now.date():
            raise ValueError("Selected date cannot be in the future.")
        date_str = target_date.strftime("%Y%m%d")
        is_today = target_date.date() == now.date()
        table_name = f"{intraday_prefix}{date_str}" if is_today else f"events_{date_str}"
        csv_path = _csv_export_path(project_id, dataset_id, table_name)
        json_path = _json_export_path(project_id, dataset_id, table_name)
        fallback_table_name: str | None = None
        fallback_csv_path: Path | None = None
        fallback_json_path: Path | None = None
        fallback_full_table_id: str | None = None
        if not is_today:
            fallback_table_name = f"{intraday_prefix}{date_str}"
            fallback_csv_path = _csv_export_path(project_id, dataset_id, fallback_table_name)
            fallback_json_path = _json_export_path(project_id, dataset_id, fallback_table_name)
            fallback_full_table_id = f"{project_id}.{dataset_id}.{fallback_table_name}"
        exports = [
            {
                "table_name": table_name,
                "full_table_id": f"{project_id}.{dataset_id}.{table_name}",
                "csv_path": csv_path,
                "json_path": json_path,
                "date": target_date,
                "fallback_table_name": fallback_table_name,
                "fallback_full_table_id": fallback_full_table_id,
                "fallback_csv_path": fallback_csv_path,
                "fallback_json_path": fallback_json_path,
            }
        ]
        display_label = target_date.strftime("%B %d, %Y")
        return exports, display_label, csv_path
    raise ValueError("Selected date range is not supported yet.")


@dataclass
class CachedSummaryArtifacts:
    dataset: Dataset
    job: SummaryJob
    report: SummaryReport


@dataclass
class CachedSummaryResult:
    summary: dict[str, Any]
    filter_note: str
    job: SummaryJob
    report: SummaryReport


def _resolve_intraday_timestamp_payload(
    summary: dict[str, Any] | None,
    *,
    job: SummaryJob | None = None,
) -> tuple[str | None, str | None]:
    if not summary or not summary.get("intraday_active"):
        return None, None
    timestamp = _parse_timestamp(summary.get("intraday_last_updated_at"))
    if timestamp is None and job is not None:
        timestamp = _parse_timestamp(getattr(job, "finished_at", None))
    if timestamp is None:
        timestamp = _now_utc()
    return timestamp.isoformat(timespec="seconds"), _format_elapsed_label(timestamp)


def _coerce_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _append_cached_suffix(note: str | None, job: SummaryJob) -> str:
    timestamp = job.finished_at
    if timestamp is not None:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        suffix = f"Cached summary generated on {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}."
    else:
        suffix = "Cached summary reused."
    if not note:
        return suffix
    trimmed = note.rstrip()
    if trimmed.endswith((".", "!", "?")):
        return f"{trimmed} {suffix}"
    return f"{trimmed}. {suffix}"


def _assemble_summary_from_report(artifacts: CachedSummaryArtifacts) -> CachedSummaryResult | None:
    report = artifacts.report
    summary_payload = report.raw_summary_json
    if isinstance(summary_payload, str):
        try:
            summary_payload = json.loads(summary_payload)
        except json.JSONDecodeError:
            summary_payload = None
    if isinstance(summary_payload, dict):
        summary_data: dict[str, Any] = copy.deepcopy(summary_payload)
    else:
        summary_data = {}

    if report.summary_mode == "csv":
        summary_data.setdefault("mode", "csv")
        if report.sections_json and "csv_sections" not in summary_data:
            summary_data["csv_sections"] = report.sections_json
        narrative_payload = report.narrative_json or {}
        if "csv_report_text" not in summary_data and isinstance(narrative_payload, dict):
            text_value = narrative_payload.get("text")
            if text_value:
                summary_data["csv_report_text"] = text_value
        if "ga4_summary" not in summary_data and report.total_events is not None:
            summary_data["ga4_summary"] = {"total_events": report.total_events}
    if report.dataset_snapshot and "dataset" not in summary_data:
        summary_data["dataset"] = report.dataset_snapshot
    if "csv_sections" not in summary_data:
        summary_data["csv_sections"] = []

    intraday_flag = bool(summary_data.get("intraday_active"))
    if not intraday_flag:
        intraday_flag = bool(getattr(artifacts.job, "intraday_active", False))
    summary_data["intraday_active"] = intraday_flag
    if intraday_flag and "intraday_last_updated_at" not in summary_data:
        finished_at = _parse_timestamp(getattr(artifacts.job, "finished_at", None))
        if finished_at is not None:
            summary_data["intraday_last_updated_at"] = finished_at.isoformat(timespec="seconds")

    if not summary_data:
        return None

    filter_note = _append_cached_suffix(artifacts.job.filter_note, artifacts.job)

    return CachedSummaryResult(
        summary=summary_data,
        filter_note=filter_note,
        job=artifacts.job,
        report=report,
    )


def _find_cached_summary_artifacts(
    range_key: str,
    *,
    project_id: str,
    dataset_id: str,
    session: Session,
) -> CachedSummaryArtifacts | None:
    dataset_stmt = select(Dataset).where(
        Dataset.project_id == project_id,
        Dataset.dataset_id == dataset_id,
    )
    dataset_record = session.execute(dataset_stmt).scalar_one_or_none()
    if dataset_record is None:
        return None

    job_stmt = (
        select(SummaryJob)
        .where(
            SummaryJob.dataset_id == dataset_record.id,
            SummaryJob.range_key == range_key,
            SummaryJob.status == "completed",
        )
        .order_by(SummaryJob.finished_at.desc().nullslast(), SummaryJob.started_at.desc().nullslast())
        .options(joinedload(SummaryJob.report))
    )
    job_record = session.execute(job_stmt).scalars().first()
    if job_record is None or job_record.report is None:
        return None
    return CachedSummaryArtifacts(dataset=dataset_record, job=job_record, report=job_record.report)


def _load_form_defaults() -> dict[str, object]:
    return {
        "dataset": _default_dataset(),
        "intraday_prefix": os.environ.get("BIGQUERY_INTRADAY_PREFIX", "events_intraday_"),
        "location": os.environ.get("BIGQUERY_LOCATION", ""),
        "max_numeric": _env_int("BIGQUERY_MAX_NUMERIC_COLUMNS", 6),
        "max_categorical": _env_int("BIGQUERY_MAX_CATEGORICAL_COLUMNS", 4),
        "top_values": _env_int("BIGQUERY_TOP_VALUES", 5),
        "all_tables": _env_bool("BIGQUERY_INCLUDE_ALL_TABLES", False),
        "project": os.environ.get("BIGQUERY_PROJECT_OVERRIDE"),
        "credentials_file": os.environ.get("BIGQUERY_CREDENTIALS_FILE")
        or app.config["DEFAULT_CREDENTIALS_FILE"],
        "week_days": _week_range_days(),
    }


class ProgressTracker:
    """Utility to normalise progress reporting for streaming responses."""

    def __init__(
        self,
        total_units: int,
        callback: Callable[[str, str, float], None] | None,
    ) -> None:
        self.total_units = max(total_units, 1)
        self.callback = callback
        self.completed_units = 0.0

    def _emit(self, stage: str, message: str, units_offset: float) -> None:
        if not self.callback:
            return
        units_offset = max(0.0, units_offset)
        ratio = (self.completed_units + units_offset) / self.total_units
        ratio = max(0.0, min(ratio, 1.0))
        progress_value = 0.05 + 0.9 * ratio
        progress_value = max(0.0, min(progress_value, 1.0))
        self.callback(stage, message, progress_value)

    def update(self, stage: str, message: str, offset: float = 0.0) -> None:
        self._emit(stage, message, offset)

    def complete_unit(self, stage: str, message: str) -> None:
        self.completed_units = min(self.total_units, self.completed_units + 1)
        self._emit(stage, message, 0.0)

    @property
    def current_progress(self) -> float:
        ratio = self.completed_units / self.total_units
        ratio = max(0.0, min(ratio, 1.0))
        progress_value = 0.05 + 0.9 * ratio
        return max(0.0, min(progress_value, 1.0))


def build_summary_for_range(
    selected_range: str,
    form_defaults: dict[str, object],
    options: DatasetSummaryOptions,
    progress_callback: Callable[[str, str, float], None] | None = None,
    db_session: Session | None = None,
    *,
    force_refresh: bool = False,
) -> tuple[dict, str | None]:
    now = _now_utc()
    normalized_range = selected_range.lower()
    project_id, dataset_id = _split_dataset(str(form_defaults["dataset"]))
    intraday_prefix = str(form_defaults["intraday_prefix"])
    exports, range_label, summary_csv_path = _resolve_exports(
        normalized_range,
        now=now,
        project_id=project_id,
        dataset_id=dataset_id,
        week_days=int(form_defaults["week_days"]),
        intraday_prefix=intraday_prefix,
    )
    persistence_mode = persistence_service.mode
    db_enabled = db_session is not None and persistence_mode == "DB"
    logger.info(
        "Summary run started for %s.%s range=%s exports=%d mode=%s",
        project_id,
        dataset_id,
        normalized_range,
        len(exports),
        persistence_mode,
    )

    location = str(form_defaults["location"]).strip() or None
    project_override = str(form_defaults["project"]) if form_defaults["project"] else None
    credentials_path = str(form_defaults["credentials_file"]) if form_defaults["credentials_file"] else None
    week_days = int(form_defaults["week_days"])
    job_week_days = week_days if normalized_range == "last7days" else 1

    external_progress_callback = progress_callback
    job_record: SummaryJob | None = None
    dataset_record: Dataset | None = None

    def emit(stage: str, message: str, progress_value: float) -> None:
        if db_enabled and job_record and stage != "complete":
            try:
                job_record.progress = float(progress_value)
                if job_record.status not in {"running", "completed", "failed"}:
                    job_record.status = "running"
                db_session.add(job_record)  # type: ignore[union-attr]
                db_session.commit()  # type: ignore[union-attr]
            except SQLAlchemyError:
                db_session.rollback()  # type: ignore[union-attr]
                raise
        if external_progress_callback:
            external_progress_callback(stage, message, progress_value)

    try:
        if db_enabled and db_session is not None:
            dataset_stmt = select(Dataset).where(
                Dataset.project_id == project_id,
                Dataset.dataset_id == dataset_id,
            )
            dataset_record = db_session.execute(dataset_stmt).scalar_one_or_none()
            if dataset_record is None:
                dataset_record = Dataset(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    location=location,
                    intraday_prefix=intraday_prefix,
                    credentials_path=credentials_path,
                    default_week_days=week_days,
                )
                db_session.add(dataset_record)
                logger.info(
                    "Created dataset record for %s.%s",
                    project_id,
                    dataset_id,
                )
            else:
                dataset_record.location = location
                dataset_record.intraday_prefix = intraday_prefix
                dataset_record.credentials_path = credentials_path
                dataset_record.default_week_days = week_days
                logger.debug(
                    "Updated dataset record for %s.%s",
                    project_id,
                    dataset_id,
                )
            db_session.commit()

            removed_jobs = _purge_existing_summary_jobs(
                db_session,
                dataset_record,
                normalized_range,
                statuses=("completed", "failed"),
            )
            if removed_jobs:
                logger.debug(
                    "Purged %d prior job(s) before creating new summary job.",
                    removed_jobs,
                )

            job_record = SummaryJob(
                dataset_id=dataset_record.id,
                range_key=normalized_range,
                week_days=job_week_days,
                status="running",
                started_at=now,
                progress=0.0,
                options_json=_jsonify(asdict(options)),
            )
            db_session.add(job_record)
            db_session.commit()
            logger.info(
                "Created summary job %s for dataset=%s.%s range=%s",
                job_record.id,
                project_id,
                dataset_id,
                normalized_range,
            )

        emit("start", f"Preparing {range_label.lower()} request", 0.02)
        total_units = len(exports) + (1 if normalized_range == "last7days" else 0) + 1
        tracker = ProgressTracker(total_units=total_units, callback=emit)

        bigquery_service: BigQueryService | None = None
        notes: list[str] = []
        csv_paths_for_summary: list[Path] = []
        latest_export_timestamp: datetime | None = None
        intraday_fallback_detected = False

        for export in exports:
            primary_table_name = str(export["table_name"])
            candidates: list[dict[str, object]] = [
                {
                    "table_name": primary_table_name,
                    "full_table_id": str(export["full_table_id"]),
                    "csv_path": Path(export["csv_path"]),
                    "json_path": Path(export["json_path"]),
                }
            ]

            fallback_table_name = export.get("fallback_table_name")
            fallback_full_table_id = export.get("fallback_full_table_id")
            fallback_csv_path = export.get("fallback_csv_path")
            fallback_json_path = export.get("fallback_json_path")
            if (
                fallback_table_name
                and fallback_full_table_id
                and fallback_csv_path
                and fallback_json_path
            ):
                fallback_entry = {
                    "table_name": str(fallback_table_name),
                    "full_table_id": str(fallback_full_table_id),
                    "csv_path": Path(fallback_csv_path),
                    "json_path": Path(fallback_json_path),
                }
                candidates.append(fallback_entry)

            if force_refresh:
                selected_candidate = candidates[0]
            else:
                selected_candidate = next((c for c in candidates if Path(c["csv_path"]).exists()), None)
                if selected_candidate is None:
                    selected_candidate = candidates[0]

            table_name = str(selected_candidate["table_name"])
            full_table_id = str(selected_candidate["full_table_id"])
            csv_path = Path(selected_candidate["csv_path"])
            json_path = Path(selected_candidate["json_path"])
            table_kind = _table_kind_from_name(table_name)
            if table_kind == "intraday":
                intraday_fallback_detected = True

            using_fallback = table_name != primary_table_name
            status_bits: list[str] = []

            logger.info(
                "Preparing export for table %s (kind=%s, needs_csv=%s, fallback=%s)",
                full_table_id,
                table_kind,
                not csv_path.exists(),
                using_fallback,
            )

            tracker.update("export", f"Checking local cache for {full_table_id}", 0.1)

            csv_rows: int | None = None
            json_rows: int | None = None

            needs_export = force_refresh or not csv_path.exists()

            if needs_export:
                if force_refresh:
                    candidate_queue = candidates
                else:
                    candidate_queue = [selected_candidate] + [
                        c for c in candidates if c is not selected_candidate
                    ]
                export_attempted = False
                for idx, candidate in enumerate(candidate_queue):
                    candidate_table_name = str(candidate["table_name"])
                    candidate_full_id = str(candidate["full_table_id"])
                    candidate_csv_path = Path(candidate["csv_path"])
                    candidate_json_path = Path(candidate["json_path"])
                    candidate_kind = _table_kind_from_name(candidate_table_name)

                    if bigquery_service is None:
                        client = _build_client(
                            credentials_file=str(form_defaults["credentials_file"]),
                            scopes=DEFAULT_SCOPES,
                            project_override=project_override,
                        )
                        bigquery_service = BigQueryService(client)
                        emit("note", "Connected to BigQuery", tracker.current_progress)

                    tracker.update(
                        "export",
                        (
                            f"Refreshing {candidate_full_id}"
                            if force_refresh
                            else f"Exporting {candidate_full_id} to CSV"
                        ),
                        0.3 + idx * 0.05,
                    )
                    try:
                        if force_refresh:
                            for stale_path in (candidate_csv_path, candidate_json_path):
                                try:
                                    stale_path.unlink()
                                except FileNotFoundError:
                                    pass
                        csv_rows = bigquery_service.export_table_to_csv(
                            candidate_full_id,
                            location=location,
                            output_path=candidate_csv_path,
                        )
                        table_name = candidate_table_name
                        full_table_id = candidate_full_id
                        csv_path = candidate_csv_path
                        json_path = candidate_json_path
                        table_kind = candidate_kind
                        using_fallback = table_name != primary_table_name
                        export_attempted = True
                        break
                    except BaseCloudError as export_error:
                        if not _is_not_found_error(export_error) or idx == len(candidate_queue) - 1:
                            raise
                        logger.warning(
                            "Primary table %s not available; trying fallback %s",
                            candidate_full_id,
                            candidate_queue[idx + 1]["full_table_id"],
                        )
                        continue

                if not export_attempted:
                    raise RuntimeError("Failed to export any candidate table for summary generation.")

                action_label = "refreshed" if force_refresh else "exported"
                status_bits.append(f"{action_label} CSV {csv_path.name} ({csv_rows:,} rows)")
                tracker.update(
                    "export",
                    (
                        f"Refreshed {csv_rows:,} rows from {full_table_id}"
                        if force_refresh
                        else f"Downloaded {csv_rows:,} rows from {full_table_id}"
                    ),
                    0.6,
                )
            else:
                status_bits.append(f"reused CSV {csv_path.name}")
                tracker.update(
                    "export",
                    f"Reusing cached CSV for {full_table_id}",
                    0.4,
                )

            json_refresh_needed = force_refresh or not json_path.exists()
            if json_refresh_needed:
                if force_refresh and json_path.exists():
                    try:
                        json_path.unlink()
                    except FileNotFoundError:
                        pass
                tracker.update(
                    "export",
                    (
                        f"Refreshing JSON cache for {full_table_id}"
                        if force_refresh
                        else f"Building JSON cache for {full_table_id}"
                    ),
                    0.85,
                )
                json_rows = _write_json_from_csv(csv_path, json_path)
                json_action = "refreshed" if force_refresh else "generated"
                status_bits.append(f"{json_action} JSON {json_path.name} ({json_rows:,} rows)")
            else:
                status_bits.append(f"reused JSON {json_path.name}")

            try:
                candidate_timestamp = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
                if latest_export_timestamp is None or candidate_timestamp > latest_export_timestamp:
                    latest_export_timestamp = candidate_timestamp
            except FileNotFoundError:
                pass

            if using_fallback and export.get("fallback_table_name"):
                status_bits.append("daily table missing; using intraday snapshot")
                intraday_fallback_detected = True

            csv_paths_for_summary.append(csv_path)

            note = f"{full_table_id}: {'; '.join(status_bits)}."
            notes.append(note)

            if db_enabled and db_session is not None and job_record is not None:
                export_record = SummaryExport(
                    job_id=job_record.id,
                    table_name=table_name,
                    full_table_id=full_table_id,
                    target_date=export.get("date").date() if isinstance(export.get("date"), datetime) else None,
                    table_kind=table_kind,
                    reused_cache=csv_rows is None,
                    csv_row_count=csv_rows,
                    json_row_count=json_rows,
                    csv_path=str(csv_path),
                    json_path=str(json_path),
                    exported_at=_now_utc(),
                    notes=note,
                    intraday_fallback=using_fallback and bool(export.get("fallback_table_name")),
                )
                db_session.add(export_record)
                db_session.commit()
                logger.debug(
                    "Recorded export %s csv_rows=%s json_rows=%s (intraday=%s)",
                    full_table_id,
                    csv_rows,
                    json_rows,
                    export_record.intraday_fallback,
                )

            tracker.complete_unit("export", f"Prepared data for {full_table_id}")
            emit("note", note, tracker.current_progress)

        if intraday_fallback_detected:
            refresh_hint = (
                "Daily GA4 table not yet published; using intraday snapshot. Run the summary again once the final table is available."
            )
            notes.append(refresh_hint)
            emit("note", refresh_hint, tracker.current_progress)

        if len(csv_paths_for_summary) == 1:
            actual_csv_path = csv_paths_for_summary[0]
            if actual_csv_path != summary_csv_path:
                logger.debug(
                    "Using intraday CSV %s as summary source (replacing %s).",
                    actual_csv_path,
                    summary_csv_path,
                )
                summary_csv_path = actual_csv_path

        if normalized_range == "last7days":
            tracker.update("combine", "Combining daily exports", 0.3)
            combined_rows = _combine_csv_files(csv_paths_for_summary, summary_csv_path)
            combination_note = (
                f"Combined {len(exports)}-day export into {summary_csv_path.name} ({combined_rows:,} rows)."
            )
            notes.append(combination_note)
            tracker.complete_unit("combine", "Combined exports into summary CSV")
            emit("note", combination_note, tracker.current_progress)
            logger.info(
                "Combined %d exports into %s rows=%d",
                len(exports),
                summary_csv_path,
                combined_rows,
            )

        if persistence_mode == "DB":
            _exit_csv_mode()
        _enter_csv_mode(summary_csv_path)
        tracker.update("summary", "Generating dataset summary", 0.3)
        service = DatasetSummaryService(None)
        summary = service.build_summary(dataset=None, tables=[], options=options)
        if isinstance(summary, dict):
            summary["intraday_active"] = intraday_fallback_detected
        tracker.complete_unit("summary", f"Summary ready for {service.csv_path.name}")

        summary_note = f"CSV mode active: summarising {service.csv_path.name}."
        details = " ".join(notes)
        filter_note = (
            f"{range_label}: {details} {summary_note}" if details else f"{range_label}: {summary_note}"
        )

        product_purchase_stats = _compute_product_purchase_stats(csv_paths_for_summary)
        summary["product_purchase_stats"] = product_purchase_stats

        if summary.get("intraday_active"):
            if latest_export_timestamp is None:
                for candidate_path in csv_paths_for_summary:
                    try:
                        candidate_ts = datetime.fromtimestamp(candidate_path.stat().st_mtime, tz=timezone.utc)
                    except FileNotFoundError:
                        continue
                    if latest_export_timestamp is None or candidate_ts > latest_export_timestamp:
                        latest_export_timestamp = candidate_ts
            timestamp_value = latest_export_timestamp or _now_utc()
            summary.setdefault("intraday_last_updated_at", timestamp_value.isoformat(timespec="seconds"))

        if db_enabled and db_session is not None and job_record is not None:
            job_record.intraday_active = intraday_fallback_detected
            if dataset_record is not None:
                dataset_record.intraday_active = intraday_fallback_detected
                db_session.add(dataset_record)
            ga4_summary = summary.get("ga4_summary") or {}
            total_events_value = ga4_summary.get("total_events")
            if isinstance(total_events_value, (int, float, Decimal)):
                total_events_payload = int(total_events_value)
            else:
                total_events_payload = None
            report_record = SummaryReport(
                job_id=job_record.id,
                summary_mode=summary.get("mode") or "csv",
                csv_summary_path=str(summary_csv_path),
                total_events=total_events_payload,
                dataset_snapshot=_jsonify(summary.get("dataset")),
                sections_json=_jsonify(summary.get("csv_sections")),
                narrative_json=_jsonify(
                    {"text": summary.get("csv_report_text")} if summary.get("csv_report_text") else None
                ),
                raw_summary_json=_jsonify(summary),
            )
            job_record.filter_note = filter_note
            job_record.status = "completed"
            job_record.progress = 1.0
            job_record.finished_at = _now_utc()
            job_record.error_message = None
            db_session.add(report_record)
            db_session.add(job_record)
            db_session.commit()
            removed_after = _purge_existing_summary_jobs(
                db_session,
                dataset_record,
                normalized_range,
                statuses=("completed", "failed"),
                exclude_job_id=job_record.id,
            )
            if removed_after:
                logger.debug(
                    "Removed %d older completed job(s) after finishing job %s",
                    removed_after,
                    job_record.id,
                )
            logger.info(
                "Summary job %s completed successfully (events=%s)",
                job_record.id,
                total_events_payload,
            )

        emit("note", summary_note, tracker.current_progress)
        emit("complete", "Summary generated successfully", 1.0)

        return summary, filter_note
    except Exception as exc:
        if db_enabled and db_session is not None and job_record is not None:
            job_record.status = "failed"
            job_record.error_message = str(exc)
            job_record.finished_at = _now_utc()
            job_record.intraday_active = intraday_fallback_detected
            if dataset_record is not None:
                dataset_record.intraday_active = intraday_fallback_detected
                db_session.add(dataset_record)
            try:
                db_session.add(job_record)
                db_session.commit()
            except SQLAlchemyError:
                db_session.rollback()
        logger.exception(
            "Summary run failed for %s.%s range=%s: %s",
            project_id,
            dataset_id,
            normalized_range,
            exc,
        )
        raise
    finally:
        _exit_csv_mode()


def _get_openai_client_for_chat() -> OpenAI:
    """Return a shared OpenAI client for the assistant."""
    global _OPENAI_CLIENT
    if _OPENAI_CLIENT is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured; AI assistant is unavailable.")
        _OPENAI_CLIENT = OpenAI(api_key=api_key)
    return _OPENAI_CLIENT


def _get_pinecone_index_for_chat():
    """Return a cached Pinecone index instance for similarity search."""
    if not PINECONE_INDEX_NAME:
        raise RuntimeError("PINECONE_INDEX_NAME is not configured; AI assistant is unavailable.")
    if not PINECONE_ENVIRONMENT:
        raise RuntimeError("PINECONE_ENVIRONMENT is not configured; AI assistant is unavailable.")

    global _PINECONE_CLIENT
    if _PINECONE_CLIENT is None:
        api_key = os.environ.get("PINECONE_API_KEY")
        if not api_key:
            raise RuntimeError("PINECONE_API_KEY is not configured; AI assistant is unavailable.")
        _PINECONE_CLIENT = Pinecone(api_key=api_key, environment=PINECONE_ENVIRONMENT)

    cached = _PINECONE_INDEX_CACHE.get(PINECONE_INDEX_NAME)
    if cached is not None:
        return cached
    try:
        index = _PINECONE_CLIENT.Index(PINECONE_INDEX_NAME)  # type: ignore[union-attr]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Unable to access Pinecone index '{PINECONE_INDEX_NAME}': {exc}") from exc
    _PINECONE_INDEX_CACHE[PINECONE_INDEX_NAME] = index
    return index


def _embed_question(text: str) -> list[float]:
    client = _get_openai_client_for_chat()
    response = client.embeddings.create(model=EMBED_MODEL, input=text)
    return response.data[0].embedding


def _query_similar_jobs(question: str) -> list[dict[str, Any]]:
    vector = _embed_question(question)
    index = _get_pinecone_index_for_chat()
    try:
        query_response = index.query(  # type: ignore[attr-defined]
            namespace=PINECONE_NAMESPACE,
            vector=vector,
            top_k=CHAT_TOP_K,
            include_metadata=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Pinecone query failed: {exc}") from exc

    matches = getattr(query_response, "matches", None) or query_response.get("matches", [])
    results: list[dict[str, Any]] = []
    for match in matches:
        metadata = getattr(match, "metadata", None) or match.get("metadata") or {}
        candidate_id = metadata.get("job_id") or getattr(match, "id", None) or match.get("id")
        if not candidate_id:
            continue
        try:
            job_uuid = uuid.UUID(str(candidate_id))
        except ValueError:
            continue
        score = getattr(match, "score", None) or metadata.get("score")
        results.append(
            {
                "job_id": job_uuid,
                "score": float(score) if score is not None else 0.0,
                "metadata": metadata,
            }
        )
    logger.debug("Assistant Pinecone matches for '%s': %s", question, results)
    return results


def _load_summary_jobs(session: Session, job_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, SummaryJob]:
    if not job_ids:
        return {}
    stmt = (
        select(SummaryJob)
        .options(joinedload(SummaryJob.report), joinedload(SummaryJob.dataset))
        .where(SummaryJob.id.in_(tuple(job_ids)))
    )
    records = session.execute(stmt).scalars().unique().all()
    return {record.id: record for record in records if record.report is not None}


def _format_ga4_metrics(payload: dict[str, Any]) -> str:
    metric_order = [
        ("total_events", "Total events"),
        ("unique_users", "Unique users"),
        ("unique_sessions", "Unique sessions"),
        ("engaged_sessions", "Engaged sessions"),
        ("total_revenue", "Revenue (USD)"),
        ("conversions", "Conversions"),
    ]
    parts: list[str] = []
    for key, label in metric_order:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            formatted = f"{value:,.0f}"
        else:
            formatted = str(value)
        parts.append(f"{label}: {formatted}")
    return "; ".join(parts)


def _build_summary_context_block(job: SummaryJob) -> str:
    dataset_label = (
        f"{job.dataset.project_id}.{job.dataset.dataset_id}"
        if job.dataset
        else "unknown dataset"
    )
    finished_at = job.finished_at.isoformat(timespec="seconds") if job.finished_at else "unknown"
    summary_payload: Any = job.report.raw_summary_json
    if isinstance(summary_payload, str):
        try:
            summary_payload = json.loads(summary_payload)
        except json.JSONDecodeError:
            summary_payload = {"raw_summary": summary_payload}

    lines = [
        f"Summary ID: {job.id}",
        f"Dataset: {dataset_label}",
        f"Range: {job.range_key}",
        f"Completed at: {finished_at}",
    ]
    if job.filter_note:
        lines.append(f"Notes: {job.filter_note}")

    intraday_flag = bool(summary_payload.get("intraday_active") or job.intraday_active)
    if intraday_flag:
        last_updated = summary_payload.get("intraday_last_updated_at")
        if last_updated:
            lines.append(f"Intraday snapshot last updated at {last_updated}.")
        else:
            lines.append("Intraday snapshot: true.")

    csv_report_text = summary_payload.get("csv_report_text")
    if isinstance(csv_report_text, str) and csv_report_text.strip():
        trimmed = csv_report_text.strip()
        lines.append("Narrative:")
        lines.append(trimmed)

    ga4_summary = summary_payload.get("ga4_summary")
    if isinstance(ga4_summary, dict):
        summary_line = _format_ga4_metrics(ga4_summary)
        if summary_line:
            lines.append(f"Key metrics: {summary_line}")

    product_stats = summary_payload.get("product_purchase_stats")
    if isinstance(product_stats, dict):
        most = product_stats.get("most_purchased")
        if isinstance(most, list) and most:
            top_items = ", ".join(
                f"{entry.get('name')} ({entry.get('count')})"
                for entry in most[:5]
                if isinstance(entry, dict) and entry.get("name")
            )
            if top_items:
                lines.append(f"Top purchased products: {top_items}.")

    sections = summary_payload.get("csv_sections")
    if isinstance(sections, list):
        for section in sections[:3]:
            if not isinstance(section, dict):
                continue
            title = section.get("title") or section.get("heading")
            summary_text = section.get("summary") or section.get("narrative")
            if title and summary_text:
                lines.append(f"{title}: {summary_text}")

    return "\n".join(lines)


def _generate_chat_response(question: str) -> dict[str, Any]:
    matches = _query_similar_jobs(question)
    if not matches:
        return {
            "answer": (
                "I could not find any cached GA4 summaries that relate to that question. "
                "Try regenerating recent summaries or adjust the timeframe you're asking about."
            ),
            "highlights": [],
            "followups": CHAT_SUGGESTIONS[:3],
            "sources": [],
        }

    session = None
    try:
        session = persistence_service.get_session()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Database session unavailable: {exc}") from exc

    try:
        job_map = _load_summary_jobs(session, [match["job_id"] for match in matches])
    finally:
        session.close()

    contexts: list[str] = []
    sources: list[dict[str, Any]] = []
    for match in matches:
        job = job_map.get(match["job_id"])
        if not job or not job.report:
            continue
        contexts.append(_build_summary_context_block(job))
        sources.append(
            {
                "job_id": str(job.id),
                "range_key": job.range_key,
                "finished_at": job.finished_at.isoformat(timespec="seconds") if job.finished_at else None,
                "score": match["score"],
                "dataset": (
                    f"{job.dataset.project_id}.{job.dataset.dataset_id}" if job.dataset else None
                ),
            }
        )

    if not contexts:
        logger.debug("Assistant: no context assembled for question '%s' (matches=%s)", question, matches)
        return {
            "answer": (
                "The assistant could not load the supporting summaries required to answer that question. "
                "Please regenerate the summaries and try again."
            ),
            "highlights": [],
            "followups": CHAT_SUGGESTIONS[:3],
            "sources": [],
        }

    context_text = "\n\n---\n\n".join(contexts)
    logger.debug(
        "Assistant context for '%s' (first 400 chars): %s",
        question,
        context_text[:400],
    )
    user_content = (
        f"User question: {question}\n\n"
        "Use only the following GA4 summary context when answering. "
        "If the context does not contain the required details, say so.\n\n"
        f"{context_text}"
    )

    client = _get_openai_client_for_chat()
    try:
        completion = client.chat.completions.create(
            model=CHAT_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": CHAT_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"OpenAI chat completion failed: {exc}") from exc
    logger.debug("Assistant OpenAI response meta: %s", completion)

    message = completion.choices[0].message.content if completion.choices else None
    answer_payload: dict[str, Any] = {}
    if message:
        try:
            answer_payload = json.loads(message)
        except json.JSONDecodeError:
            answer_payload = {"answer": message}

    answer_text = str(answer_payload.get("answer") or "").strip()
    highlights = answer_payload.get("highlights")
    followups = answer_payload.get("followups")

    if not isinstance(highlights, list):
        highlights = []
    if not isinstance(followups, list) or not followups:
        followups = CHAT_SUGGESTIONS[:3]

    if not answer_text:
        answer_text = (
            "I was unable to produce a detailed answer from the current summaries. "
            "Please refine the question or refresh the underlying data."
        )

    return {
        "answer": answer_text,
        "highlights": [str(item) for item in highlights if isinstance(item, str)],
        "followups": [str(item) for item in followups if isinstance(item, str)][:3],
        "sources": sources,
    }


@app.get("/assistant")
def assistant_home() -> str:
    user_name = os.environ.get("AI_ASSISTANT_USER_NAME", "Tracey")
    return render_template(
        "assistant.html",
        user_name=user_name,
        suggestions=CHAT_SUGGESTIONS,
    )


@app.post("/assistant/chat")
def assistant_chat():
    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Please enter a question for the assistant."}), 400
    try:
        response_payload = _generate_chat_response(question)
    except RuntimeError as exc:
        logger.error("Assistant request failed: %s", exc)
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected assistant error: %s", exc)
        return jsonify({"error": "Unexpected error while generating a response."}), 500
    return jsonify(response_payload)


@app.route("/", methods=["GET", "POST"])
def index():
    form_defaults = _load_form_defaults()
    project_id, dataset_id = _split_dataset(str(form_defaults["dataset"]))

    summary: dict | None = None
    error_message: str | None = None
    filter_note: str | None = None
    selected_range = "today"
    summary_generated = False
    db_session: Session | None = None
    db_error: str | None = None
    options = DatasetSummaryOptions(
        location=(form_defaults["location"] or None),
        max_numeric_columns=max(0, int(form_defaults["max_numeric"])),
        max_categorical_columns=max(0, int(form_defaults["max_categorical"])),
        max_top_values=max(1, int(form_defaults["top_values"])),
    )
    debug_mode = _env_bool("DEBUG_MODE", False)
    selected_specific_date = ""
    current_date_iso = _now_utc().date().isoformat()
    force_refresh_checked = False
    intraday_hint = ""
    intraday_last_updated: str | None = None
    intraday_last_updated_label: str | None = None

    try:
        if _use_database_persistence():
            try:
                db_session = persistence_service.get_session()
            except Exception as exc:  # noqa: BLE001
                db_error = f"Database connection failed: {exc}"
                logger.error(db_error)
        else:
            logger.debug("Persistence mode CSV; skipping database session acquisition.")

        if request.method == "POST":
            custom_date_value = (request.form.get("custom_date") or "").strip()
            selected_range_value = request.form.get("date_range", "today")
            force_refresh_checked = _coerce_truthy(request.form.get("force_refresh"))
            force_refresh = force_refresh_checked
            if custom_date_value:
                selected_specific_date = custom_date_value
                selected_range = f"date:{custom_date_value}"
            else:
                selected_range = selected_range_value
            selected_range = selected_range.lower()
            if request.form.get("action") != "generate":
                return render_template(
                    "index.html",
                    form=form_defaults,
                    summary=None,
                    filter_note=None,
                    error_message=None,
                    selected_date_range=selected_range,
                    selected_specific_date=selected_specific_date,
                    current_date=current_date_iso,
                    force_refresh_checked=force_refresh_checked,
                    summary_generated=False,
                    debug_mode=debug_mode,
                    intraday_hint=intraday_hint,
                    intraday_last_updated=intraday_last_updated,
                    intraday_last_updated_label=intraday_last_updated_label,
                )
            if db_error and db_session is None:
                error_message = db_error
            else:
                try:
                    cached_result: CachedSummaryResult | None = None
                    if (
                        not force_refresh
                        and _use_database_persistence()
                        and db_session is not None
                    ):
                        artifacts = _find_cached_summary_artifacts(
                            selected_range,
                            project_id=project_id,
                            dataset_id=dataset_id,
                            session=db_session,
                        )
                        if artifacts is not None:
                            cached_result = _assemble_summary_from_report(artifacts)
                            if cached_result is not None:
                                logger.info(
                                    "Reused cached summary for %s.%s range=%s job=%s",
                                    project_id,
                                    dataset_id,
                                    selected_range,
                                    cached_result.job.id,
                                )
                    if cached_result is not None:
                        summary = cached_result.summary
                        filter_note = cached_result.filter_note
                        summary_generated = True
                    else:
                        summary, filter_note = build_summary_for_range(
                            selected_range=selected_range,
                            form_defaults=form_defaults,
                            options=options,
                            db_session=db_session if _use_database_persistence() else None,
                            force_refresh=force_refresh,
                        )
                        summary_generated = True
                    if summary_generated:
                        fallback_flag = (
                            cached_result.job.intraday_active
                            if cached_result is not None and cached_result.job is not None
                            else None
                        )
                        intraday_hint = _intraday_hint(summary, fallback_flag)
                        intraday_last_updated, intraday_last_updated_label = _resolve_intraday_timestamp_payload(
                            summary,
                            job=cached_result.job if cached_result is not None else None,
                        )
                except IntradayTableNotFound as exc:
                    error_message = f"{exc} Hint: try another --intraday-date or tick 'All tables'."
                    logger.warning(error_message)
                except FileNotFoundError as exc:
                    error_message = f"Credentials file error: {exc}"
                    logger.error(error_message)
                except ValueError as exc:
                    error_message = str(exc)
                    logger.error("Validation error: %s", error_message)
                except BaseCloudError as exc:
                    error_message = f"BigQuery request failed: {exc}"
                    logger.exception(error_message)
                except Exception as exc:  # noqa: BLE001
                    error_message = f"Unexpected error: {exc}"
                    logger.exception("Unexpected error in index handler: %s", exc)

        if db_error and not error_message:
            error_message = db_error

        if not selected_specific_date and selected_range.startswith("date:"):
            selected_specific_date = selected_range.split(":", 1)[1]

        return render_template(
            "index.html",
            form=form_defaults,
            summary=summary,
            filter_note=filter_note,
            error_message=error_message,
            selected_date_range=selected_range,
            selected_specific_date=selected_specific_date,
            current_date=current_date_iso,
            force_refresh_checked=force_refresh_checked,
            summary_generated=summary_generated,
            debug_mode=debug_mode,
            intraday_hint=intraday_hint,
            intraday_last_updated=intraday_last_updated,
            intraday_last_updated_label=intraday_last_updated_label,
        )
    finally:
        if db_session is not None:
            db_session.close()
            logger.debug("Closed request-scoped database session.")


@app.post("/api/check-summary")
def check_summary():
    payload = request.get_json(silent=True) or {}
    date_str = str(payload.get("date") or "").strip()
    if not date_str:
        return jsonify({"error": "Missing date value."}), 400

    normalized_range = f"date:{date_str}".lower()
    form_defaults = _load_form_defaults()
    project_id, dataset_id = _split_dataset(str(form_defaults["dataset"]))
    intraday_prefix = str(form_defaults["intraday_prefix"])

    try:
        exports, _, summary_csv_path = _resolve_exports(
            normalized_range,
            now=_now_utc(),
            project_id=project_id,
            dataset_id=dataset_id,
            week_days=int(form_defaults["week_days"]),
            intraday_prefix=intraday_prefix,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    def _path_exists(path_like: Path | str) -> bool:
        candidate = path_like if isinstance(path_like, Path) else Path(str(path_like))
        return candidate.exists()

    paths_available = all(_path_exists(export["csv_path"]) for export in exports)
    summary_available = _path_exists(summary_csv_path)
    available = paths_available or summary_available

    db_summary_available = False
    cached_finished_at: datetime | None = None
    if persistence_service.is_database_mode():
        session: Session | None = None
        try:
            session = persistence_service.get_session()
            artifacts = _find_cached_summary_artifacts(
                normalized_range,
                project_id=project_id,
                dataset_id=dataset_id,
                session=session,
            )
            if artifacts is not None:
                db_summary_available = True
                cached_finished_at = artifacts.job.finished_at
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to check database summary availability: %s", exc)
        finally:
            if session is not None:
                session.close()
    available = available or db_summary_available

    return jsonify(
        {
            "available": available,
            "range_key": normalized_range,
            "has_local_cache": paths_available or summary_available,
            "has_database_summary": db_summary_available,
            "last_completed_at": cached_finished_at.isoformat() if cached_finished_at else None,
        }
    )


@app.post("/api/progress-summary")
def progress_summary() -> Response:
    payload = request.get_json(silent=True) or {}
    selected_range = str(payload.get("date_range") or "today")
    custom_date = str(payload.get("custom_date") or "").strip()
    if custom_date:
        selected_range = f"date:{custom_date}"
    selected_range = selected_range.lower()
    force_refresh = _coerce_truthy(payload.get("force_refresh"))
    form_defaults = _load_form_defaults()
    project_id, dataset_id = _split_dataset(str(form_defaults["dataset"]))
    options = DatasetSummaryOptions(
        location=(form_defaults["location"] or None),
        max_numeric_columns=max(0, int(form_defaults["max_numeric"])),
        max_categorical_columns=max(0, int(form_defaults["max_categorical"])),
        max_top_values=max(1, int(form_defaults["top_values"])),
    )
    debug_mode = _env_bool("DEBUG_MODE", False)

    event_queue: SimpleQueue[dict | None] = SimpleQueue()

    def enqueue(event: dict | None) -> None:
        event_queue.put(event)

    def progress_cb(stage: str, message: str, progress_value: float) -> None:
        if stage == "note":
            enqueue({"type": "note", "message": message, "progress": progress_value})
        else:
            enqueue(
                {
                    "type": "status",
                    "stage": stage,
                    "message": message,
                    "progress": progress_value,
                }
            )

    def worker() -> None:
        session: Session | None = None
        try:
            with app.app_context():
                logger.info("Worker started for range=%s", selected_range)
                if _use_database_persistence():
                    try:
                        session = persistence_service.get_session()
                    except Exception as exc:  # noqa: BLE001
                        enqueue({"type": "error", "message": f"Database connection failed: {exc}"})
                        logger.error("Worker failed to acquire DB session: %s", exc)
                        return
                if (
                    not force_refresh
                    and _use_database_persistence()
                    and session is not None
                ):
                    artifacts = _find_cached_summary_artifacts(
                        selected_range,
                        project_id=project_id,
                        dataset_id=dataset_id,
                        session=session,
                    )
                    if artifacts is not None:
                        cached_result = _assemble_summary_from_report(artifacts)
                        if cached_result is not None:
                            enqueue(
                                {
                                    "type": "status",
                                    "stage": "cache",
                                    "message": "Loading cached summary…",
                                    "progress": 0.2,
                                }
                            )
                            enqueue(
                                {
                                    "type": "note",
                                    "message": cached_result.filter_note,
                                    "progress": 0.65,
                                }
                            )
                            intraday_hint_value = _intraday_hint(
                                cached_result.summary,
                                cached_result.job.intraday_active if cached_result.job else None,
                            )
                            (
                                intraday_last_updated_value,
                                intraday_last_updated_label,
                            ) = _resolve_intraday_timestamp_payload(
                                cached_result.summary,
                                job=cached_result.job,
                            )
                            summary_html = render_template(
                                "summary_content.html",
                                summary=cached_result.summary,
                                filter_note=cached_result.filter_note,
                                error_message=None,
                                summary_generated=True,
                                selected_date_range=selected_range,
                                form=form_defaults,
                                debug_mode=debug_mode,
                                intraday_hint=intraday_hint_value,
                                intraday_last_updated=intraday_last_updated_value,
                                intraday_last_updated_label=intraday_last_updated_label,
                            )
                            enqueue(
                                {
                                    "type": "status",
                                    "stage": "cache",
                                    "message": "Cached summary ready.",
                                    "progress": 0.95,
                                }
                            )
                            enqueue(
                                {
                                    "type": "complete",
                                    "html": summary_html,
                                    "hint": intraday_hint_value,
                                    "intraday_last_updated": intraday_last_updated_value,
                                    "intraday_last_updated_label": intraday_last_updated_label,
                                }
                            )
                            logger.info(
                                "Worker reused cached summary for %s.%s range=%s job=%s",
                                project_id,
                                dataset_id,
                                selected_range,
                                cached_result.job.id,
                            )
                            return
                summary, filter_note = build_summary_for_range(
                    selected_range=selected_range,
                    form_defaults=form_defaults,
                    options=options,
                    progress_callback=progress_cb,
                    db_session=session if _use_database_persistence() else None,
                    force_refresh=force_refresh,
                )
                intraday_hint_value = _intraday_hint(summary)
                (
                    intraday_last_updated_value,
                    intraday_last_updated_label,
                ) = _resolve_intraday_timestamp_payload(summary)
                summary_html = render_template(
                    "summary_content.html",
                    summary=summary,
                    filter_note=filter_note,
                    error_message=None,
                    summary_generated=True,
                    selected_date_range=selected_range,
                    form=form_defaults,
                    debug_mode=debug_mode,
                    intraday_hint=intraday_hint_value,
                    intraday_last_updated=intraday_last_updated_value,
                    intraday_last_updated_label=intraday_last_updated_label,
                )
            enqueue(
                {
                    "type": "complete",
                    "html": summary_html,
                    "hint": intraday_hint_value,
                    "intraday_last_updated": intraday_last_updated_value,
                    "intraday_last_updated_label": intraday_last_updated_label,
                }
            )
        except IntradayTableNotFound as exc:
            enqueue({"type": "error", "message": f"{exc} Hint: try another date range or enable all tables."})
        except FileNotFoundError as exc:
            enqueue({"type": "error", "message": f"Credentials file error: {exc}"})
        except ValueError as exc:
            enqueue({"type": "error", "message": str(exc)})
        except BaseCloudError as exc:
            enqueue({"type": "error", "message": f"BigQuery request failed: {exc}"})
            logger.exception("BigQuery request failed during worker execution: %s", exc)
        except Exception as exc:  # noqa: BLE001
            enqueue({"type": "error", "message": f"Unexpected error: {exc}"})
            logger.exception("Unexpected worker error: %s", exc)
        finally:
            if session is not None:
                session.close()
                logger.debug("Closed worker database session.")
            enqueue(None)

    Thread(target=worker, daemon=True).start()

    @stream_with_context
    def stream():
        while True:
            event = event_queue.get()
            if event is None:
                break
            yield json.dumps(event) + "\n"

    return Response(stream(), mimetype="application/x-ndjson")


if __name__ == "__main__":
    # Bind to all interfaces so the app works in container/remote dev setups.
    app.run(debug=True, host="0.0.0.0", port=5500)
