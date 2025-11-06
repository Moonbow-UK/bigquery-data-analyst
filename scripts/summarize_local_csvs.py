#!/usr/bin/env python3
from __future__ import annotations

"""
Generate GA4-style summaries from locally exported CSV files and persist them to the database.

The command walks a date range, reusing the same dataset defaults as the Flask app.
It skips dates whose CSV exports are missing and records the outcome in the log.
Use `--days` to include additional days going backwards from the start date,
`--refresh-json` to regenerate cached NDJSON exports from the local CSVs without contacting BigQuery,
or `--force-refresh` for a full CSV+JSON rebuild via BigQuery.
Pass `--all` to summarise every dated CSV found under `var/exports/csv`.
Use `--dry-run` to preview what would be refreshed or generated without touching files.
Checksums keep CSV and JSON exports in sync; disable the validation logic with --skip-integrity-check.
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # ensure local packages (bqtools, app) are importable
    sys.path.insert(0, str(ROOT))

from bqtools.config import load_environment
from bqtools.storage import (
    CSV_EXPORT_DIR,
    EXPORT_ROOT,
    ensure_local_export_file,
    export_file_exists,
    list_remote_export_relpaths,
    sync_export_artifact,
)
from bqtools.services.dataset_summary import DatasetSummaryOptions

import app

LOGGER = logging.getLogger("csv_range_summary")
CHECKSUM_MANIFEST_PATH = EXPORT_ROOT / "checksums.json"


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:  # pragma: no cover - defensive parsing
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from exc


def _iter_dates(start: date, end: date) -> Iterable[date]:
    current = start
    step = timedelta(days=1)
    while current <= end:
        yield current
        current += step


@dataclass()
class _ProcessResult:
    processed: list[date]
    skipped: list[tuple[date, str]]
    failed: list[tuple[date, str]]


def _configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    LOGGER.setLevel(numeric_level)
    # Align the Flask-side logger with the CLI verbosity for consistent output.
    logging.getLogger("summary_app").setLevel(numeric_level)


def _collect_available_exports(exports: list[dict]) -> list[Path]:
    available: list[Path] = []
    for export in exports:
        csv_path = Path(export["csv_path"])
        if export_file_exists(csv_path):
            available.append(csv_path)
            continue
        fallback_path = export.get("fallback_csv_path")
        if fallback_path:
            fallback = Path(fallback_path)
            if export_file_exists(fallback):
                available.append(fallback)
    return available


def _discover_local_dates(
    *,
    project_id: str,
    dataset_id: str,
    intraday_prefix: str,
) -> list[date]:
    prefix = f"{project_id}_{dataset_id}_"
    discovered: set[date] = set()
    filenames = {path.name for path in CSV_EXPORT_DIR.glob("*.csv")}
    for rel_path in list_remote_export_relpaths(suffix=".csv"):
        parts = rel_path.parts
        if parts and parts[0] != "csv":
            continue
        filenames.add(rel_path.name)
    for name in filenames:
        if not name.startswith(prefix) or not name.endswith(".csv"):
            continue
        table_part = name[len(prefix) : -4]
        candidate: str | None = None
        if table_part.startswith("events_") and len(table_part) >= len("events_") + 8:
            suffix = table_part[len("events_") :]
            if len(suffix) == 8 and suffix.isdigit():
                candidate = suffix
        elif table_part.startswith(intraday_prefix) and len(table_part) >= len(intraday_prefix) + 8:
            suffix = table_part[len(intraday_prefix) :]
            if len(suffix) == 8 and suffix.isdigit():
                candidate = suffix
        if candidate:
            try:
                discovered.add(datetime.strptime(candidate, "%Y%m%d").date())
            except ValueError:
                continue
    return sorted(discovered, reverse=True)


def _load_checksum_manifest() -> dict[str, dict[str, str | None]]:
    if not export_file_exists(CHECKSUM_MANIFEST_PATH):
        return {}
    ensure_local_export_file(CHECKSUM_MANIFEST_PATH)
    try:
        with CHECKSUM_MANIFEST_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("Failed to read checksum manifest %s: %s", CHECKSUM_MANIFEST_PATH, exc)
        return {}
    entries = data.get("entries") if isinstance(data, dict) else None
    if isinstance(entries, dict):
        coerced: dict[str, dict[str, str | None]] = {}
        for key, value in entries.items():
            if isinstance(key, str) and isinstance(value, dict):
                coerced[key] = {
                    "csv_checksum": value.get("csv_checksum"),
                    "json_path": value.get("json_path"),
                    "json_checksum": value.get("json_checksum"),
                    "updated_at": value.get("updated_at"),
                }
        return coerced
    return {}


def _save_checksum_manifest(manifest: dict[str, dict[str, str | None]]) -> None:
    payload = {
        "entries": manifest,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    CHECKSUM_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKSUM_MANIFEST_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    tmp_path.replace(CHECKSUM_MANIFEST_PATH)
    sync_export_artifact(CHECKSUM_MANIFEST_PATH)


def _compute_checksum(path: Path, *, chunk_size: int = 1 << 20) -> str:
    import hashlib

    hasher = hashlib.md5()
    ensure_local_export_file(path)
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def _collect_candidate_pairs(exports: Sequence[dict]) -> list[tuple[Path, Path]]:
    pairs: list[tuple[Path, Path]] = []
    seen: set[tuple[Path, Path]] = set()
    for export in exports:
        csv_path = Path(export["csv_path"])
        json_path = Path(export["json_path"])
        primary = (csv_path, json_path)
        if primary not in seen:
            pairs.append(primary)
            seen.add(primary)
        fallback_csv = export.get("fallback_csv_path")
        fallback_json = export.get("fallback_json_path")
        if fallback_csv and fallback_json:
            fallback_pair = (Path(fallback_csv), Path(fallback_json))
            if fallback_pair not in seen:
                pairs.append(fallback_pair)
                seen.add(fallback_pair)
    return pairs


def _identify_out_of_sync_pairs(
    pairs: Sequence[tuple[Path, Path]],
    manifest: dict[str, dict[str, str | None]],
) -> set[Path]:
    refresh_paths: set[Path] = set()
    for csv_path, json_path in pairs:
        if not export_file_exists(csv_path):
            continue
        key = str(csv_path)
        record = manifest.get(key)
        try:
            csv_checksum = _compute_checksum(csv_path)
        except OSError as exc:
            LOGGER.warning("Unable to read CSV %s for checksum: %s", csv_path, exc)
            continue
        json_exists = export_file_exists(json_path)
        json_checksum: str | None = None
        if json_exists:
            try:
                json_checksum = _compute_checksum(json_path)
            except OSError as exc:
                LOGGER.warning("Unable to read JSON %s for checksum: %s", json_path, exc)
                json_exists = False

        csv_changed = record is None or record.get("csv_checksum") != csv_checksum
        json_changed = False
        if json_exists:
            if record is None or record.get("json_checksum") != json_checksum:
                json_changed = True
        else:
            if record and record.get("json_checksum"):
                json_changed = True

        if csv_changed:
            LOGGER.info("Detected CSV change for %s; JSON will be regenerated.", csv_path)
        if json_changed and not csv_changed:
            LOGGER.info("Detected JSON mismatch for %s; forcing regeneration.", json_path)

        if (csv_changed or json_changed) and json_exists:
            refresh_paths.add(json_path)
    return refresh_paths


def _refresh_json_exports(
    exports: Sequence[dict],
    *,
    refresh: bool,
    paths_to_refresh: set[Path] | None = None,
) -> None:
    targeted_paths = paths_to_refresh or set()
    if not refresh and not targeted_paths:
        return
    seen: set[Path] = set()
    for export in exports:
        for key in ("json_path", "fallback_json_path"):
            path_like = export.get(key)
            if not path_like:
                continue
            path = Path(path_like)
            if path in seen:
                continue
            if not export_file_exists(path):
                if refresh:
                    seen.add(path)
                continue
            should_refresh = refresh or path in targeted_paths
            if not should_refresh:
                continue
            try:
                path.unlink()
            except OSError as exc:
                LOGGER.warning("Unable to remove cached JSON %s: %s", path, exc)
            else:
                LOGGER.info("Removed cached JSON %s to force regeneration.", path)
            seen.add(path)


def _process_dates(
    dates: Iterable[date],
    *,
    form_defaults: dict[str, object],
    project_id: str,
    dataset_id: str,
    force_refresh: bool,
    refresh_json: bool,
    manifest: dict[str, dict[str, str | None]],
    integrity_enabled: bool,
    dry_run: bool,
) -> _ProcessResult:
    processed: list[date] = []
    skipped: list[tuple[date, str]] = []
    failed: list[tuple[date, str]] = []

    if not app.persistence_service.is_database_mode():
        raise RuntimeError(
            f"Database persistence required; current mode is {app.persistence_service.mode}."
        )

    options = DatasetSummaryOptions(
        location=(form_defaults["location"] or None),
        max_numeric_columns=max(0, int(form_defaults["max_numeric"])),
        max_categorical_columns=max(0, int(form_defaults["max_categorical"])),
        max_top_values=max(1, int(form_defaults["top_values"])),
    )
    now = app._now_utc()
    intraday_prefix = str(form_defaults["intraday_prefix"])
    week_days = int(form_defaults["week_days"])

    session = None
    if not dry_run:
        session = app.persistence_service.get_session()
    try:
        for target_date in dates:
            range_key = f"date:{target_date.isoformat()}"
            LOGGER.info("Processing %s.%s for %s", project_id, dataset_id, range_key)
            try:
                exports, range_label, _ = app._resolve_exports(
                    range_key,
                    now=now,
                    project_id=project_id,
                    dataset_id=dataset_id,
                    week_days=week_days,
                    intraday_prefix=intraday_prefix,
                )
            except ValueError as exc:
                LOGGER.warning("Skipping %s: %s", target_date.isoformat(), exc)
                skipped.append((target_date, str(exc)))
                continue

            candidate_pairs = _collect_candidate_pairs(exports)
            paths_to_refresh = set()
            if integrity_enabled:
                paths_to_refresh = _identify_out_of_sync_pairs(candidate_pairs, manifest)

            if dry_run:
                if paths_to_refresh:
                    for json_path in sorted(paths_to_refresh):
                        LOGGER.info(
                            "Dry run: JSON %s out of sync; would regenerate from CSV.",
                            json_path,
                        )
            else:
                _refresh_json_exports(
                    exports,
                    refresh=refresh_json,
                    paths_to_refresh=paths_to_refresh,
                )
            available_csvs = _collect_available_exports(exports)
            if not available_csvs and not force_refresh:
                expected = ", ".join(str(Path(export["csv_path"])) for export in exports)
                LOGGER.warning(
                    "Skipping %s: CSV export not found (checked %s).",
                    target_date.isoformat(),
                    expected or "no paths generated",
                )
                skipped.append((target_date, "missing CSV export"))
                continue

            if dry_run:
                if force_refresh:
                    LOGGER.info(
                        "Dry run: would export fresh CSV/JSON for %s (force refresh).",
                        range_label,
                    )
                LOGGER.info(
                    "Dry run: would summarise %s using %d CSV file(s).",
                    range_label,
                    len(available_csvs),
                )
                processed.append(target_date)
                continue

            try:
                LOGGER.info(
                    "Generating summary for %s using %d CSV file(s)%s.",
                    range_label,
                    len(available_csvs),
                    " (forcing refresh)" if force_refresh else "",
                )
                summary, _ = app.build_summary_for_range(
                    selected_range=range_key,
                    form_defaults=form_defaults,
                    options=options,
                    progress_callback=None,
                    db_session=session,
                    force_refresh=force_refresh,
                )
                ga4_summary = summary.get("ga4_summary") if isinstance(summary, dict) else None
                total_events = ga4_summary.get("total_events") if isinstance(ga4_summary, dict) else None
                LOGGER.info(
                    "Stored summary for %s (%s events).",
                    target_date.isoformat(),
                    f"{int(total_events):,}" if isinstance(total_events, (int, float)) else "n/a",
                )
                _record_checksums(manifest, candidate_pairs)
                try:
                    _save_checksum_manifest(manifest)
                except OSError as exc:
                    LOGGER.warning(
                        "Unable to write checksum manifest after %s: %s",
                        target_date.isoformat(),
                        exc,
                    )
                processed.append(target_date)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to summarise %s: %s", target_date.isoformat(), exc)
                failed.append((target_date, str(exc)))
                try:
                    if session is not None:
                        session.rollback()
                except Exception:  # noqa: BLE001
                    LOGGER.exception("Failed to rollback session after error.")
    finally:
        if session is not None:
            session.close()

    return _ProcessResult(processed=processed, skipped=skipped, failed=failed)


def _record_checksums(
    manifest: dict[str, dict[str, str | None]],
    pairs: Sequence[tuple[Path, Path]],
) -> None:
    for csv_path, json_path in pairs:
        key = str(csv_path)
        if not export_file_exists(csv_path):
            if key in manifest:
                LOGGER.debug("Removing manifest entry for missing CSV %s.", csv_path)
                manifest.pop(key, None)
            continue
        try:
            csv_checksum = _compute_checksum(csv_path)
        except OSError as exc:
            LOGGER.warning("Unable to record checksum for CSV %s: %s", csv_path, exc)
            continue
        json_checksum: str | None = None
        if export_file_exists(json_path):
            try:
                json_checksum = _compute_checksum(json_path)
            except OSError as exc:
                LOGGER.warning("Unable to record checksum for JSON %s: %s", json_path, exc)
        manifest[key] = {
            "csv_checksum": csv_checksum,
            "json_path": str(json_path),
            "json_checksum": json_checksum,
            "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Summarise locally exported GA4 CSV files for a date range and persist the results to the database."
        )
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        help="Anchor date to process (YYYY-MM-DD). Required unless --all is used.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        help="Last date to process (YYYY-MM-DD, inclusive). Defaults to --start-date when omitted.",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to include starting from --start-date and going backwards (default: 1 when omitted).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Summarise every dated CSV under var/exports/csv (ignores --start-date/--end-date/--days).",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Rebuild CSV exports even if a cached file exists (may hit BigQuery).",
    )
    parser.add_argument(
        "--refresh-json",
        action="store_true",
        help="Delete cached JSON exports so they are regenerated from the CSVs without re-querying BigQuery.",
    )
    parser.add_argument(
        "--skip-integrity-check",
        action="store_true",
        help="Skip checksum validation between CSV and JSON exports.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview actions without exporting data, generating summaries, or touching files.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ...). Defaults to INFO.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    _configure_logging(args.log_level)
    load_environment()
    form_defaults = app._load_form_defaults()
    project_id, dataset_id = app._split_dataset(str(form_defaults["dataset"]))
    intraday_prefix = str(form_defaults["intraday_prefix"])
    manifest = _load_checksum_manifest()
    integrity_enabled = not args.skip_integrity_check
    if not integrity_enabled:
        LOGGER.info("Checksum integrity validation disabled by flag.")
    if args.dry_run:
        LOGGER.info("Dry run enabled – no files will be modified.")

    if args.all:
        target_dates = _discover_local_dates(
            project_id=project_id,
            dataset_id=dataset_id,
            intraday_prefix=intraday_prefix,
        )
        if not target_dates:
            LOGGER.warning("No local CSV exports found under %s.", CSV_EXPORT_DIR)
    else:
        if args.start_date is None:
            parser.error("--start-date is required unless --all is used.")
        if args.end_date and args.days is not None:
            parser.error("--days cannot be combined with --end-date.")
        start_date = args.start_date
        target_dates: list[date]
        if args.days is not None:
            if args.days <= 0:
                parser.error("--days must be a positive integer.")
            target_dates = [start_date - timedelta(days=offset) for offset in range(args.days)]
        else:
            end_date = args.end_date or start_date
            lower, upper = sorted((start_date, end_date))
            date_seq = list(_iter_dates(lower, upper))
            if end_date < start_date:
                target_dates = list(reversed(date_seq))
            else:
                target_dates = date_seq

    # Remove duplicates while preserving order.
    seen_dates: set[date] = set()
    ordered_dates: list[date] = []
    for dt in target_dates:
        if dt not in seen_dates:
            ordered_dates.append(dt)
            seen_dates.add(dt)

    if not ordered_dates:
        LOGGER.warning("No dates to process.")
        return 2

    exit_code = 0
    result: _ProcessResult | None = None
    try:
        result = _process_dates(
            ordered_dates,
            form_defaults=form_defaults,
            project_id=project_id,
            dataset_id=dataset_id,
            force_refresh=args.force_refresh,
            refresh_json=args.refresh_json,
            manifest=manifest,
            integrity_enabled=integrity_enabled,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Fatal error: %s", exc)
        exit_code = 1
    else:
        LOGGER.info(
            "Finished. processed=%d skipped=%d failed=%d",
            len(result.processed),
            len(result.skipped),
            len(result.failed),
        )
        if result.skipped:
            for target_date, reason in result.skipped:
                LOGGER.warning("Skipped %s: %s", target_date.isoformat(), reason)
        if result.failed:
            for target_date, message in result.failed:
                LOGGER.error("Failed %s: %s", target_date.isoformat(), message)
            exit_code = 1
        elif not result.processed:
            exit_code = 2
    finally:
        if not args.dry_run:
            try:
                _save_checksum_manifest(manifest)
            except OSError as exc:
                LOGGER.warning("Unable to write checksum manifest: %s", exc)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
