#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import os
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from pathlib import Path
from queue import SimpleQueue
from threading import Thread
from typing import Any, Callable

from flask import Flask, Response, render_template, request, stream_with_context
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

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

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "change-me")
app.config["DEFAULT_CREDENTIALS_FILE"] = str(default_credentials_file())


summary_api = create_summary_blueprint(
    SummaryAPIConfig(default_credentials=default_credentials_file())
)
app.register_blueprint(summary_api, url_prefix="/api")

LOG_DIR = Path("var/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

logger = logging.getLogger("summary_app")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
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

persistence_service = PersistenceService(logger=logger.getChild("persistence"))

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


def _table_kind_from_name(table_name: str) -> str:
    lowered = table_name.lower()
    if "intraday" in lowered:
        return "intraday"
    if "events" in lowered:
        return "daily"
    return "unknown"


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
) -> tuple[list[dict[str, Path | str | datetime]], str, Path]:
    normalized_key = range_key.lower()
    if normalized_key == "today":
        target_date = now
        table_name = _table_name_for_range("today", reference_time=target_date)
        csv_path = Path(f"{project_id}_{dataset_id}_{table_name}.csv")
        json_path = Path(f"{project_id}_{dataset_id}_{table_name}.json")
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
        table_name = _table_name_for_range("yesterday", reference_time=target_date)
        csv_path = Path(f"{project_id}_{dataset_id}_{table_name}.csv")
        json_path = Path(f"{project_id}_{dataset_id}_{table_name}.json")
        exports = [
            {
                "table_name": table_name,
                "full_table_id": f"{project_id}.{dataset_id}.{table_name}",
                "csv_path": csv_path,
                "json_path": json_path,
                "date": target_date,
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
            csv_path = Path(f"{project_id}_{dataset_id}_{table_name}.csv")
            json_path = Path(f"{project_id}_{dataset_id}_{table_name}.json")
            exports.append(
                {
                    "table_name": table_name,
                    "full_table_id": f"{project_id}.{dataset_id}.{table_name}",
                    "csv_path": csv_path,
                    "json_path": json_path,
                    "date": target_date,
                }
            )
        summary_csv = Path(f"{project_id}_{dataset_id}_events_last{days_to_fetch}days.csv")
        day_label = "day" if days_to_fetch == 1 else "days"
        return exports, f"Last {days_to_fetch} {day_label}", summary_csv
    raise ValueError("Selected date range is not supported yet.")


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
) -> tuple[dict, str | None]:
    now = _now_utc()
    normalized_range = selected_range.lower()
    project_id, dataset_id = _split_dataset(str(form_defaults["dataset"]))
    exports, range_label, summary_csv_path = _resolve_exports(
        normalized_range,
        now=now,
        project_id=project_id,
        dataset_id=dataset_id,
        week_days=int(form_defaults["week_days"]),
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
    intraday_prefix = str(form_defaults["intraday_prefix"])
    week_days = int(form_defaults["week_days"])

    external_progress_callback = progress_callback
    job_record: SummaryJob | None = None
    dataset_record: Dataset | None = None

    def emit(stage: str, message: str, progress_value: float) -> None:
        if db_enabled and job_record and stage != "complete":
            try:
                job_record.progress = float(progress_value)
                if job_record.status != "running":
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

            job_record = SummaryJob(
                dataset_id=dataset_record.id,
                range_key=normalized_range,
                week_days=week_days,
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

        for export in exports:
            csv_path = Path(export["csv_path"])
            json_path = Path(export["json_path"])
            full_table_id = str(export["full_table_id"])
            table_name = str(export.get("table_name") or csv_path.stem)
            table_kind = _table_kind_from_name(table_name)
            csv_paths_for_summary.append(csv_path)
            logger.info(
                "Preparing export for table %s (kind=%s, needs_csv=%s)",
                full_table_id,
                table_kind,
                not csv_path.exists(),
            )

            status_bits: list[str] = []
            tracker.update("export", f"Checking local cache for {full_table_id}", 0.1)

            needs_csv = not csv_path.exists()
            csv_rows: int | None = None
            json_rows: int | None = None
            export_record: SummaryExport | None = None

            if db_enabled and db_session is not None and job_record is not None:
                export_record = SummaryExport(
                    job_id=job_record.id,
                    table_name=table_name,
                    full_table_id=full_table_id,
                    target_date=export.get("date").date() if isinstance(export.get("date"), datetime) else None,
                    table_kind=table_kind,
                    reused_cache=not needs_csv,
                    csv_path=str(csv_path),
                    json_path=str(json_path),
                    exported_at=_now_utc(),
                )
                db_session.add(export_record)
                db_session.commit()

            if needs_csv:
                tracker.update("export", f"Exporting {full_table_id} to CSV", 0.3)
                if bigquery_service is None:
                    client = _build_client(
                        credentials_file=str(form_defaults["credentials_file"]),
                        scopes=DEFAULT_SCOPES,
                        project_override=project_override,
                    )
                    bigquery_service = BigQueryService(client)
                    emit("note", "Connected to BigQuery", tracker.current_progress)
                csv_rows = bigquery_service.export_table_to_csv(
                    full_table_id,
                    location=location,
                    output_path=csv_path,
                )
                status_bits.append(f"exported CSV {csv_path.name} ({csv_rows:,} rows)")
                tracker.update("export", f"Downloaded {csv_rows:,} rows from {full_table_id}", 0.6)
            else:
                status_bits.append(f"reused CSV {csv_path.name}")
                tracker.update("export", f"Reusing cached CSV for {full_table_id}", 0.4)

            if not json_path.exists():
                tracker.update("export", f"Building JSON cache for {full_table_id}", 0.85)
                json_rows = _write_json_from_csv(csv_path, json_path)
                status_bits.append(f"generated JSON {json_path.name} ({json_rows:,} rows)")
            else:
                status_bits.append(f"reused JSON {json_path.name}")

            note = f"{full_table_id}: {'; '.join(status_bits)}."
            notes.append(note)

            if db_enabled and export_record is not None and db_session is not None:
                export_record.reused_cache = not needs_csv
                export_record.csv_row_count = csv_rows
                export_record.json_row_count = json_rows
                export_record.notes = note
                export_record.exported_at = export_record.exported_at or _now_utc()
                db_session.add(export_record)
                db_session.commit()
                logger.debug(
                    "Recorded export %s csv_rows=%s json_rows=%s",
                    full_table_id,
                    csv_rows,
                    json_rows,
                )

            tracker.complete_unit("export", f"Prepared data for {full_table_id}")
            emit("note", note, tracker.current_progress)

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
        tracker.complete_unit("summary", f"Summary ready for {service.csv_path.name}")

        summary_note = f"CSV mode active: summarising {service.csv_path.name}."
        details = " ".join(notes)
        filter_note = (
            f"{range_label}: {details} {summary_note}" if details else f"{range_label}: {summary_note}"
        )

        if db_enabled and db_session is not None and job_record is not None:
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


@app.route("/", methods=["GET", "POST"])
def index():
    form_defaults = _load_form_defaults()

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
            selected_range = request.form.get("date_range", "today").lower()
            if request.form.get("action") != "generate":
                return render_template(
                    "index.html",
                    form=form_defaults,
                    summary=None,
                    filter_note=None,
                    error_message=None,
                    selected_date_range=selected_range,
                    summary_generated=False,
                )
            if db_error and db_session is None:
                error_message = db_error
            else:
                try:
                    summary, filter_note = build_summary_for_range(
                        selected_range=selected_range,
                        form_defaults=form_defaults,
                        options=options,
                        db_session=db_session if _use_database_persistence() else None,
                    )
                    summary_generated = True
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

        return render_template(
            "index.html",
            form=form_defaults,
            summary=summary,
            filter_note=filter_note,
            error_message=error_message,
            selected_date_range=selected_range,
            summary_generated=summary_generated,
        )
    finally:
        if db_session is not None:
            db_session.close()
            logger.debug("Closed request-scoped database session.")


@app.post("/api/progress-summary")
def progress_summary() -> Response:
    payload = request.get_json(silent=True) or {}
    selected_range = str(payload.get("date_range") or "today").lower()
    form_defaults = _load_form_defaults()
    options = DatasetSummaryOptions(
        location=(form_defaults["location"] or None),
        max_numeric_columns=max(0, int(form_defaults["max_numeric"])),
        max_categorical_columns=max(0, int(form_defaults["max_categorical"])),
        max_top_values=max(1, int(form_defaults["top_values"])),
    )

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
                summary, filter_note = build_summary_for_range(
                    selected_range=selected_range,
                    form_defaults=form_defaults,
                    options=options,
                    progress_callback=progress_cb,
                    db_session=session if _use_database_persistence() else None,
                )
                summary_html = render_template(
                    "summary_content.html",
                    summary=summary,
                    filter_note=filter_note,
                    error_message=None,
                    summary_generated=True,
                    selected_date_range=selected_range,
                    form=form_defaults,
                )
            enqueue({"type": "complete", "html": summary_html})
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
    app.run(debug=True, host="127.0.0.1", port=5000)
