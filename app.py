#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, render_template, request

from bqtools import (
    DEFAULT_SCOPES,
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


def _build_client(credentials_file: str, scopes: tuple[str, ...], project_override: str | None):
    return build_client(
        credentials_file=Path(credentials_file).expanduser(),
        scopes=scopes,
        project_override=project_override,
    )


@app.route("/", methods=["GET", "POST"])
def index():
    form_defaults = {
        "dataset": request.values.get("dataset", "euronics-1047.analytics_308868785"),
        "intraday_date": request.values.get("intraday_date", ""),
        "intraday_prefix": request.values.get("intraday_prefix", "events_intraday_"),
        "location": request.values.get("location", ""),
        "max_numeric": int(request.values.get("max_numeric", 6)),
        "max_categorical": int(request.values.get("max_categorical", 4)),
        "top_values": int(request.values.get("top_values", 5)),
        "all_tables": request.values.get("all_tables") == "on",
        "project": request.values.get("project") or None,
        "credentials_file": request.values.get("credentials_file")
        or app.config["DEFAULT_CREDENTIALS_FILE"],
    }

    summary: dict | None = None
    error_message: str | None = None
    filter_note: str | None = None
    options = DatasetSummaryOptions(
        location=form_defaults["location"] or None,
        max_numeric_columns=max(0, form_defaults["max_numeric"]),
        max_categorical_columns=max(0, form_defaults["max_categorical"]),
        max_top_values=max(1, form_defaults["top_values"]),
    )

    csv_mode = os.environ.get("BIGQUERY_USE_CSV", "").strip().lower() in {"1", "true", "yes", "on"}

    if request.method == "POST":
        try:
            if csv_mode:
                service = DatasetSummaryService(None)
                summary = service.build_summary(dataset=None, tables=[], options=options)
                filter_note = filter_note or f"CSV mode active: summarising {service.csv_path.name}"
            else:
                client = _build_client(
                    credentials_file=form_defaults["credentials_file"],
                    scopes=DEFAULT_SCOPES,
                    project_override=form_defaults["project"],
                )
                service = DatasetSummaryService(client)
                dataset_ref = service.resolve_dataset_ref(form_defaults["dataset"])
                dataset = client.get_dataset(dataset_ref)
                tables, filter_note = service.get_tables_for_summary(
                    dataset_ref=dataset_ref,
                    all_tables=form_defaults["all_tables"],
                    intraday_date=form_defaults["intraday_date"] or None,
                    intraday_table_prefix=form_defaults["intraday_prefix"],
                )
                summary = service.build_summary(
                    dataset=dataset,
                    tables=tables,
                    options=options,
                )
        except IntradayTableNotFound as exc:
            error_message = f"{exc} Hint: try another --intraday-date or tick 'All tables'."
        except FileNotFoundError as exc:
            error_message = f"Credentials file error: {exc}"
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
    )


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
