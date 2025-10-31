#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import SimpleQueue
from threading import Thread
from typing import Callable

from flask import Flask, Response, render_template, request, stream_with_context

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

load_environment()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "change-me")
app.config["DEFAULT_CREDENTIALS_FILE"] = str(default_credentials_file())


summary_api = create_summary_blueprint(
    SummaryAPIConfig(default_credentials=default_credentials_file())
)
app.register_blueprint(summary_api, url_prefix="/api")


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
) -> tuple[dict, str | None]:
    now = datetime.now(timezone.utc)
    project_id, dataset_id = _split_dataset(str(form_defaults["dataset"]))
    exports, range_label, summary_csv_path = _resolve_exports(
        selected_range,
        now=now,
        project_id=project_id,
        dataset_id=dataset_id,
        week_days=int(form_defaults["week_days"]),
    )

    progress_callback and progress_callback("start", f"Preparing {range_label.lower()} request", 0.02)
    total_units = len(exports) + (1 if selected_range.lower() == "last7days" else 0) + 1
    tracker = ProgressTracker(total_units=total_units, callback=progress_callback)

    location = str(form_defaults["location"]).strip() or None
    bigquery_service: BigQueryService | None = None
    notes: list[str] = []
    csv_paths_for_summary: list[Path] = []

    for export in exports:
        csv_path = Path(export["csv_path"])
        json_path = Path(export["json_path"])
        full_table_id = str(export["full_table_id"])
        csv_paths_for_summary.append(csv_path)

        status_bits: list[str] = []
        tracker.update("export", f"Checking local cache for {full_table_id}", 0.1)

        needs_csv = not csv_path.exists()
        if needs_csv:
            tracker.update("export", f"Exporting {full_table_id} to CSV", 0.3)
            if bigquery_service is None:
                client = _build_client(
                    credentials_file=str(form_defaults["credentials_file"]),
                    scopes=DEFAULT_SCOPES,
                    project_override=str(form_defaults["project"]) if form_defaults["project"] else None,
                )
                bigquery_service = BigQueryService(client)
                progress_callback and progress_callback(
                    "note", "Connected to BigQuery", tracker.current_progress
                )
            row_count = bigquery_service.export_table_to_csv(
                full_table_id,
                location=location,
                output_path=csv_path,
            )
            status_bits.append(f"exported CSV {csv_path.name} ({row_count:,} rows)")
            tracker.update("export", f"Downloaded {row_count:,} rows from {full_table_id}", 0.6)
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
        tracker.complete_unit("export", f"Prepared data for {full_table_id}")
        progress_callback and progress_callback("note", note, tracker.current_progress)

    if selected_range.lower() == "last7days":
        tracker.update("combine", "Combining daily exports", 0.3)
        combined_rows = _combine_csv_files(csv_paths_for_summary, summary_csv_path)
        combination_note = (
            f"Combined {len(exports)}-day export into {summary_csv_path.name} ({combined_rows:,} rows)."
        )
        notes.append(combination_note)
        tracker.complete_unit("combine", "Combined exports into summary CSV")
        progress_callback and progress_callback("note", combination_note, tracker.current_progress)

    os.environ["BIGQUERY_USE_CSV"] = "true"
    os.environ["BIGQUERY_USE_CSV_FILE"] = str(summary_csv_path)

    tracker.update("summary", "Generating dataset summary", 0.3)
    service = DatasetSummaryService(None)
    summary = service.build_summary(dataset=None, tables=[], options=options)
    tracker.complete_unit("summary", f"Summary ready for {service.csv_path.name}")

    summary_note = f"CSV mode active: summarising {service.csv_path.name}."
    details = " ".join(notes)
    filter_note = f"{range_label}: {details} {summary_note}" if details else f"{range_label}: {summary_note}"
    progress_callback and progress_callback("note", summary_note, tracker.current_progress)
    progress_callback and progress_callback("complete", "Summary generated successfully", 1.0)

    return summary, filter_note


@app.route("/", methods=["GET", "POST"])
def index():
    form_defaults = _load_form_defaults()

    summary: dict | None = None
    error_message: str | None = None
    filter_note: str | None = None
    selected_range = "today"
    summary_generated = False
    options = DatasetSummaryOptions(
        location=(form_defaults["location"] or None),
        max_numeric_columns=max(0, int(form_defaults["max_numeric"])),
        max_categorical_columns=max(0, int(form_defaults["max_categorical"])),
        max_top_values=max(1, int(form_defaults["top_values"])),
    )

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
        try:
            summary, filter_note = build_summary_for_range(
                selected_range=selected_range,
                form_defaults=form_defaults,
                options=options,
            )
            summary_generated = True
        except IntradayTableNotFound as exc:
            error_message = f"{exc} Hint: try another --intraday-date or tick 'All tables'."
        except FileNotFoundError as exc:
            error_message = f"Credentials file error: {exc}"
        except ValueError as exc:
            error_message = str(exc)
        except BaseCloudError as exc:
            error_message = f"BigQuery request failed: {exc}"
        except Exception as exc:  # noqa: BLE001
            error_message = f"Unexpected error: {exc}"

    return render_template(
        "index.html",
        form=form_defaults,
        summary=summary,
        filter_note=filter_note,
        error_message=error_message,
        selected_date_range=selected_range,
        summary_generated=summary_generated,
    )


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
        try:
            with app.app_context():
                summary, filter_note = build_summary_for_range(
                    selected_range=selected_range,
                    form_defaults=form_defaults,
                    options=options,
                    progress_callback=progress_cb,
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
        except Exception as exc:  # noqa: BLE001
            enqueue({"type": "error", "message": f"Unexpected error: {exc}"})
        finally:
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
