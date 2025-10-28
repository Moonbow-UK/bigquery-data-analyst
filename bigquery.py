from __future__ import annotations

"""
Command line helper for validating BigQuery access, previewing tables,
and exporting data. The heavy lifting is delegated to the shared `bqtools`
package so that future integrations (databases, chatbots, dashboards) can
reuse the same logic.
"""

import argparse
import sys
from pathlib import Path

from bqtools import DEFAULT_SCOPES, BigQueryService, build_client
from bqtools.config import default_credentials_file, load_environment


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test a BigQuery connection using Google Cloud credentials.",
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
            "BigQuery project ID to use. "
            "If omitted, the script tries to infer it from the credentials."
        ),
    )
    parser.add_argument(
        "--dataset",
        help=(
            "Optional dataset to scope table listings. "
            "Accepts either DATASET_ID or PROJECT_ID.DATASET_ID. "
            "When omitted, all datasets in the project are scanned."
        ),
    )
    parser.add_argument(
        "--query",
        default="SELECT CURRENT_TIMESTAMP() AS current_time",
        help=(
            "Query to execute for the connectivity check (ignored when --list-tables is set). "
            "Defaults to a lightweight timestamp query."
        ),
    )
    parser.add_argument(
        "--location",
        help=(
            "Optional BigQuery location/region for the query job "
            "(for example, 'US' or 'europe-west1')."
        ),
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=5,
        help=(
            "Maximum number of rows to print from query or table previews (ignored when exporting CSV)."
        ),
    )
    parser.add_argument(
        "--scope",
        dest="scopes",
        action="append",
        help=(
            "Optional OAuth scope(s) to use instead of the default BigQuery scope. "
            "You can specify this flag multiple times."
        ),
    )
    parser.add_argument(
        "--table",
        help=(
            "Preview data from the specified table. "
            "Accepts DATASET.TABLE or PROJECT.DATASET.TABLE. "
            "When set, the script runs SELECT * ... LIMIT --max-rows."
        ),
    )
    parser.add_argument(
        "--row-count",
        action="store_true",
        help=(
            "When combined with --table, return only the total row count instead of preview rows."
        ),
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help=(
            "List tables that are visible to the provided credentials. "
            "When set, the script prints dataset/table names instead of executing the test query."
        ),
    )
    parser.add_argument(
        "--csv",
        nargs="?",
        const="AUTO",
        metavar="OUTPUT",
        help=(
            "Export data to CSV. "
            "Combine with --table to write a single table to OUTPUT, "
            "or with --dataset to write one CSV per table inside OUTPUT directory. "
            "Omit OUTPUT to generate a filename automatically in the current directory."
        ),
    )
    return parser.parse_args()


def resolve_auto_csv_target(service: BigQueryService, args: argparse.Namespace) -> Path:
    if args.csv != "AUTO":
        return Path(args.csv)
    if args.table:
        normalized_table = service.normalize_table_identifier(args.table)
        safe_name = normalized_table.replace(".", "_")
        return Path.cwd() / f"{safe_name}.csv"
    if args.dataset:
        dataset_project, dataset_name = service.normalize_dataset_identifier(args.dataset)
        safe_dir = f"{dataset_project}_{dataset_name}_csv"
        return Path.cwd() / safe_dir
    raise RuntimeError("--csv requires either --table or --dataset.")


def print_rows(headers: tuple[str, ...], rows: list[list[object]]) -> None:
    if not rows:
        print("No rows returned.")
        return
    print(" | ".join(headers))
    for row in rows:
        print(" | ".join(str(value) for value in row))


def print_table_listing(table_entries: list[dict[str, str]]) -> None:
    if not table_entries:
        print("No tables found for the supplied credentials.")
        return
    current_dataset = None
    for entry in sorted(table_entries, key=lambda x: (x["project"], x["dataset"], x["table"])):
        dataset_identifier = f"{entry['project']}.{entry['dataset']}"
        if dataset_identifier != current_dataset:
            current_dataset = dataset_identifier
            print(f"Dataset: {dataset_identifier}")
        print(f"  - {entry['table']}")


def main() -> int:
    load_environment()
    args = parse_args()
    credentials_path = Path(args.credentials_file)

    try:
        scopes = tuple(args.scopes) if args.scopes else DEFAULT_SCOPES
        client = build_client(
            credentials_file=credentials_path,
            scopes=scopes,
            project_override=args.project,
        )
        service = BigQueryService(client)

        if args.table and args.list_tables:
            raise RuntimeError("Choose either --table or --list-tables, not both.")
        if args.csv and args.list_tables:
            raise RuntimeError("--csv cannot be combined with --list-tables.")
        if args.csv and args.row_count:
            raise RuntimeError("--row-count cannot be combined with --csv.")
        if args.row_count and not args.table:
            raise RuntimeError("--row-count must be used together with --table.")

        location = args.location or None

        if args.csv:
            target = resolve_auto_csv_target(service, args)
            if args.table:
                row_count = service.export_table_to_csv(
                    args.table,
                    location=location,
                    output_path=target,
                )
                print(f"CSV export complete. Wrote {row_count} row(s) to {target}")
            elif args.dataset:
                written = service.export_dataset_to_csv(
                    args.dataset,
                    location=location,
                    output_dir=target,
                )
                print(
                    f"Dataset CSV export complete. Wrote {len(written)} file(s) to {target}"
                )
                for path in written:
                    print(f"- {path}")
            else:
                raise RuntimeError("--csv requires either --table or --dataset.")
        elif args.table and args.row_count:
            count = service.fetch_row_count(
                args.table,
                location=location,
            )
            formatted = f"{count:,}"
            print(f"Row count for `{service.normalize_table_identifier(args.table)}`: {formatted}")
        elif args.table:
            preview = service.preview_table(
                args.table,
                location=location,
                max_rows=max(1, args.max_rows),
            )
            print(
                f"Preview of `{preview['table_id']}` "
                f"(showing up to {args.max_rows} row(s)):"
            )
            print_rows(preview["headers"], preview["rows"])
        elif args.list_tables:
            entries = service.list_tables(dataset_id=args.dataset)
            if args.dataset:
                dataset_project, dataset_name = service.normalize_dataset_identifier(args.dataset)
                print(f"Tables in dataset {dataset_project}.{dataset_name}:")
            print_table_listing(entries)
        else:
            result = service.run_test_query(
                args.query,
                location=location,
                max_rows=max(1, args.max_rows),
            )
            if not result["rows"]:
                print("Query completed successfully but returned no rows.")
            else:
                print(
                    f"Connection successful. Displaying up to {args.max_rows} row(s):"
                )
                print_rows(result["headers"], result["rows"])
    except Exception as exc:  # noqa: BLE001
        print(f"BigQuery connectivity test failed: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
