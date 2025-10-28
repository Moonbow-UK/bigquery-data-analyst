from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from flask import Blueprint, Request, Response, current_app, jsonify, request

from .. import (
    DEFAULT_SCOPES,
    DatasetSummaryOptions,
    DatasetSummaryService,
    IntradayTableNotFound,
    build_client,
)
from ..services.dataset_summary import BaseCloudError


@dataclass(slots=True)
class SummaryAPIConfig:
    default_credentials: Path
    scopes: tuple[str, ...] = DEFAULT_SCOPES
    project_resolver: Callable[[Request], str | None] | None = None


def create_summary_blueprint(config: SummaryAPIConfig) -> Blueprint:
    """Factory that exposes dataset summaries as a JSON API."""

    bp = Blueprint("dataset_summary_api", __name__)

    @bp.post("/summary")
    def post_summary() -> Response:
        payload = request.get_json(silent=True) or {}
        dataset = payload.get("dataset")
        if not dataset:
            return _error_response("'dataset' is required", 400)

        credentials_file = payload.get("credentials_file") or str(config.default_credentials)
        scopes = _coerce_scopes(payload.get("scopes"), config.scopes)
        project_override = payload.get("project")
        if config.project_resolver:
            project_override = project_override or config.project_resolver(request)

        options = DatasetSummaryOptions(
            location=payload.get("location"),
            max_numeric_columns=_coerce_int(payload.get("max_numeric_columns"), 6),
            max_categorical_columns=_coerce_int(payload.get("max_categorical_columns"), 4),
            max_top_values=_coerce_int(payload.get("max_top_values"), 5),
        )

        include_text = _to_bool(payload.get("include_text"), False)
        include_overview = _to_bool(payload.get("include_overview"), True)

        filter_note = None
        try:
            csv_mode = _env_flag("BIGQUERY_USE_CSV")
            if csv_mode:
                service = DatasetSummaryService(None)
                summary_struct = service.build_summary(dataset=None, tables=[], options=options)
                filter_note = f"CSV mode active: summarising {service.csv_path.name}"
            else:
                client = build_client(
                    credentials_file=Path(credentials_file),
                    scopes=scopes,
                    project_override=project_override,
                )
                service = DatasetSummaryService(client)
                dataset_ref = service.resolve_dataset_ref(dataset)
                dataset_obj = client.get_dataset(dataset_ref)
                tables, filter_note = service.get_tables_for_summary(
                    dataset_ref=dataset_ref,
                    all_tables=_to_bool(payload.get("all_tables"), False),
                    intraday_date=payload.get("intraday_date"),
                    intraday_table_prefix=payload.get("intraday_table_prefix", "events_intraday_"),
                )
                summary_struct = service.build_summary(
                    dataset=dataset_obj,
                    tables=tables,
                    options=options,
                )
            response_payload: dict[str, object] = {
                "summary": summary_struct if include_overview else {},
                "filter_note": filter_note,
            }
            if include_text:
                response_payload["summary_text"] = service.summarise_to_text(summary=summary_struct)
        except IntradayTableNotFound as exc:
            return _error_response(str(exc), 404)
        except FileNotFoundError as exc:
            return _error_response(f"Credentials file error: {exc}", 400)
        except BaseCloudError as exc:
            current_app.logger.exception("BigQuery request failed")
            return _error_response(f"BigQuery request failed: {exc}", 502)
        except Exception as exc:  # noqa: BLE001
            current_app.logger.exception("Unexpected error while building summary")
            return _error_response(f"Unexpected error: {exc}", 500)

        return jsonify(response_payload)

    return bp


def _error_response(message: str, status: int) -> Response:
    return jsonify({"error": message, "status": status}), status


def _coerce_scopes(raw_scopes: object, default: Iterable[str]) -> tuple[str, ...]:
    if raw_scopes is None:
        return tuple(default)
    if isinstance(raw_scopes, str):
        return (raw_scopes,)
    try:
        return tuple(str(scope) for scope in raw_scopes)
    except TypeError:
        return (str(raw_scopes),)


def _coerce_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return value != 0
    return bool(value)


def _env_flag(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}
