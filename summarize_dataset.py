#!/usr/bin/env python3
from __future__ import annotations

"""
CLI entrypoint that delegates dataset summarisation to the reusable
`DatasetSummaryService`. Produces the same human-readable report as the
previous implementation while exposing structured data for future features.
"""

import argparse
import os
import sys
from pathlib import Path

from bqtools import (
    DEFAULT_SCOPES,
    DatasetSummaryOptions,
    DatasetSummaryService,
    IntradayTableNotFound,
    build_client,
    generate_ga4_intraday_summary,
)
from bqtools.config import default_credentials_file, load_environment
from bqtools.services.dataset_summary import BaseCloudError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a human-friendly BigQuery dataset summary.",
    )
    parser.add_argument(
        "--credentials-file",
        default=str(default_credentials_file()),
        help=(
            "Path to the OAuth 'authorized user' JSON file or a service account key. "
            "Defaults to %(default)s or the BIGQUERY_CREDENTIALS_FILE environment variable."
        ),
    )
    parser.add_argument(
        "--project",
        help=(
            "Optional project override. "
            "When omitted, the project is inferred from the credentials."
        ),
    )
    parser.add_argument(
        "--dataset",
        default="euronics-1047.analytics_308868785",
        help=(
            "Dataset to summarise. Accepts DATASET or PROJECT.DATASET. "
            "Defaults to %(default)s."
        ),
    )
    parser.add_argument(
        "--location",
        help=(
            "Optional BigQuery location/region hint (e.g. 'EU'). "
            "Most metadata calls work without it, but it can speed up stats queries."
        ),
    )
    parser.add_argument(
        "--max-numeric-columns",
        type=int,
        default=6,
        help="Maximum number of numeric/date columns to scan per table for min/max/avg stats.",
    )
    parser.add_argument(
        "--max-categorical-columns",
        type=int,
        default=4,
        help="Maximum number of categorical columns to summarise with top values per table.",
    )
    parser.add_argument(
        "--top-values",
        type=int,
        default=5,
        help="Maximum number of top categorical values to display for each column.",
    )
    parser.add_argument(
        "--intraday-date",
        help=(
            "Restrict the summary to the intraday table for the supplied date (YYYYMMDD). "
            "Defaults to today's date when --all-tables is not provided."
        ),
    )
    parser.add_argument(
        "--intraday-table-prefix",
        default="events_intraday_",
        help=(
            "Prefix used when building the intraday table name. "
            "Ignored when --all-tables is set. Defaults to %(default)s."
        ),
    )
    parser.add_argument(
        "--all-tables",
        action="store_true",
        help="Include every table in the dataset instead of limiting to the intraday table.",
    )
    parser.add_argument(
        "--scopes",
        dest="scopes",
        action="append",
        help=(
            "Optional OAuth scope(s) to use instead of the default BigQuery scope. "
            "You can specify this flag multiple times."
        ),
    )
    return parser.parse_args()


def _env_flag(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main() -> int:
    load_environment()
    args = parse_args()
    credentials_path = Path(args.credentials_file)
    options = DatasetSummaryOptions(
        location=args.location or None,
        max_numeric_columns=max(0, args.max_numeric_columns),
        max_categorical_columns=max(0, args.max_categorical_columns),
        max_top_values=max(1, args.top_values),
    )

    csv_mode = _env_flag("BIGQUERY_USE_CSV")
    csv_file_env = os.environ.get("BIGQUERY_USE_CSV_FILE")
    if csv_mode:
        if not csv_file_env:
            print(
                "CSV mode enabled (BIGQUERY_USE_CSV) but BIGQUERY_USE_CSV_FILE was not provided.",
                file=sys.stderr,
            )
            return 1
        csv_path = Path(csv_file_env).expanduser()
        try:
            _, csv_text = generate_ga4_intraday_summary(csv_path, options)
        except FileNotFoundError as exc:
            print(f"CSV summary failed: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:  # noqa: BLE001
            print(f"CSV summary failed: {exc}", file=sys.stderr)
            return 1

        print(csv_text)
        return 0

    try:
        scopes = tuple(args.scopes) if args.scopes else DEFAULT_SCOPES
        client = build_client(
            credentials_file=credentials_path,
            scopes=scopes,
            project_override=args.project,
        )
        service = DatasetSummaryService(client)
        dataset_ref = service.resolve_dataset_ref(args.dataset)
        dataset = client.get_dataset(dataset_ref)
        tables, filter_note = service.get_tables_for_summary(
            dataset_ref=dataset_ref,
            all_tables=args.all_tables,
            intraday_date=args.intraday_date,
            intraday_table_prefix=args.intraday_table_prefix,
        )
        summary_struct = service.build_summary(
            dataset=dataset,
            tables=tables,
            options=options,
        )
        summary_text = service.summarise_to_text(summary=summary_struct)
    except IntradayTableNotFound as exc:
        print(exc, file=sys.stderr)
        print(
            "Hint: use --intraday-date YYYYMMDD for another day or --all-tables to summarise everything.",
            file=sys.stderr,
        )
        return 0
    except FileNotFoundError as exc:
        print(f"Credentials file error: {exc}", file=sys.stderr)
        return 1
    except BaseCloudError as exc:
        print(f"BigQuery request failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to generate dataset summary: {exc}", file=sys.stderr)
        return 1

    if filter_note:
        print(filter_note)
        print()
    print(summary_text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
