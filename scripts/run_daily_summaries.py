#!/usr/bin/env python3
"""Trigger daily summary generation for a date range via the Flask worker."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bqtools.config import load_environment
from bqtools.storage import CSV_EXPORT_DIR, export_modified_time
from bqtools.services.persistence import Dataset, PersistenceService, SummaryJob

LOGGER = logging.getLogger("daily_summaries")
STALE_THRESHOLD = timedelta(days=2)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ensure daily summaries exist for a range of dates."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Starting date in YYYY-MM-DD (typically yesterday).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to cover going backwards from the start date (default: 7).",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("SUMMARY_BASE_URL", "http://127.0.0.1:5500"),
        help="Base URL for the Flask app (default: %(default)s).",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Always regenerate summaries even if a completed job already exists.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Request timeout in seconds for each summary generation (default: 600).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print streaming progress messages from the server.",
    )
    parser.add_argument(
        "--auto-refresh-stale",
        action="store_true",
        help="Automatically refresh stale intraday exports (older than 2 days) without prompting.",
    )
    return parser.parse_args()


def _dataset_identifiers() -> tuple[str, str]:
    project = os.environ.get("BIGQUERY_PROJECT_ID", "euronics-1047")
    dataset_id = os.environ.get("BIGQUERY_DATASET_ID", "analytics_308868785")
    return project, dataset_id


def _intraday_prefix() -> str:
    return os.environ.get("BIGQUERY_INTRADAY_PREFIX", "events_intraday_")


def _intraday_csv_path(project: str, dataset_id: str, target_date: date) -> Path:
    export_root = CSV_EXPORT_DIR
    date_str = target_date.strftime("%Y%m%d")
    filename = f"{project}_{dataset_id}_{_intraday_prefix()}{date_str}.csv"
    return export_root / filename


def _is_stale(path: Path) -> tuple[bool, datetime | None]:
    modified = export_modified_time(path)
    if modified is None:
        return False, None
    age = datetime.now(timezone.utc) - modified
    return age > STALE_THRESHOLD, modified


def _ensure_dataset_record(session, project: str, dataset_id: str) -> Dataset | None:
    stmt = select(Dataset).where(
        Dataset.project_id == project,
        Dataset.dataset_id == dataset_id,
    )
    return session.execute(stmt).scalar_one_or_none()


def _summary_exists(session, dataset_record: Dataset | None, range_key: str) -> bool:
    if dataset_record is None:
        return False
    stmt = (
        select(SummaryJob)
        .where(
            SummaryJob.dataset_id == dataset_record.id,
            SummaryJob.range_key == range_key,
            SummaryJob.status == "completed",
        )
        .limit(1)
    )
    job = session.execute(stmt).scalar_one_or_none()
    return job is not None


def _trigger_summary(
    base_url: str,
    range_key: str,
    *,
    force_refresh: bool,
    timeout: int,
    verbose: bool,
) -> None:
    payload = {
        "date_range": range_key,
        "force_refresh": force_refresh,
    }
    url = f"{base_url.rstrip('/')}/api/progress-summary"
    LOGGER.info("Requesting summary: %s", json.dumps(payload))
    try:
        response = requests.post(url, json=payload, stream=True, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.ReadTimeout:
        LOGGER.warning(
            "Timed out waiting for %s; the worker may still complete in the background.",
            range_key,
        )
        return
    if not verbose:
        # Drain the stream quietly to allow the server to finish the job.
        for _ in response.iter_content(chunk_size=None):
            pass
        return

    for line in response.iter_lines(decode_unicode=True):
        if not line:
            continue
        LOGGER.info("%s", line)


def _compute_date_range(start_date: date, days: int) -> list[date]:
    days = max(1, days)
    dates = [start_date - timedelta(days=offset) for offset in range(days)]
    return sorted(dates)


def _confirm_stale_refresh(path: Path, modified: datetime | None, auto_refresh: bool) -> bool:
    if modified is None:
        return False
    if auto_refresh:
        LOGGER.info(
            "Intraday export %s last updated %s UTC – auto-refreshing.",
            path,
            modified.isoformat(timespec="seconds"),
        )
        return True
    prompt = (
        f"Intraday export {path} was last updated on {modified.isoformat(timespec='seconds')} UTC,"
        " more than 2 days ago. Refresh from BigQuery? [y/N]: "
    )
    try:
        response = input(prompt)
    except EOFError:
        response = ""
    if response.strip().lower() in {"y", "yes"}:
        LOGGER.info("Confirmed refresh for stale export %s.", path)
        return True
    LOGGER.info("Skipping refresh for stale export %s.", path)
    return False


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    load_environment()
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit("--start-date must be in YYYY-MM-DD format") from exc

    project_id, dataset_id = _dataset_identifiers()
    persistence_service = PersistenceService(logger=LOGGER.getChild("persistence"))
    session = None
    try:
        session = persistence_service.get_session()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Database unavailable (%s); summaries will always be generated.", exc)

    try:
        dataset_record = (
            _ensure_dataset_record(session, project_id, dataset_id)
            if session is not None
            else None
        )
        for target_date in _compute_date_range(start_date, args.days):
            range_key = f"date:{target_date.isoformat()}"
            needs_refresh = True
            if session is not None and not args.force_refresh:
                needs_refresh = not _summary_exists(session, dataset_record, range_key)
            stale_refresh = False
            stale_path = _intraday_csv_path(project_id, dataset_id, target_date)
            stale, modified = _is_stale(stale_path)
            if stale and not args.force_refresh:
                stale_refresh = _confirm_stale_refresh(stale_path, modified, args.auto_refresh_stale)
                if not stale_refresh and needs_refresh is False:
                    LOGGER.info("Summary already present for %s; skipping.", range_key)
                    continue
            elif not needs_refresh and not args.force_refresh:
                LOGGER.info("Summary already present for %s; skipping.", range_key)
                continue
            _trigger_summary(
                args.base_url,
                range_key,
                force_refresh=args.force_refresh or stale_refresh,
                timeout=args.timeout,
                verbose=args.verbose,
            )
            if session is not None:
                dataset_record = _ensure_dataset_record(session, project_id, dataset_id)
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    main()
