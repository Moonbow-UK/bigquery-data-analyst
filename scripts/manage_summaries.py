#!/usr/bin/env python3
"""Utility script to prune or reset summary persistence tables."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bqtools.config import load_environment

LOGGER = logging.getLogger("manage_summaries")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prune duplicate summary jobs or reset persistence tables."
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete all rows from summary tables (exports, reports, jobs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without executing mutations.",
    )
    return parser.parse_args()


def _load_database_url() -> str:
    load_environment()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not defined.")
    return database_url


def _prune_duplicates(engine, dry_run: bool) -> None:
    LOGGER.info("Pruning duplicate summary jobs (keeping the most recent per dataset + range).")
    delete_sql = """
        DELETE FROM summary_jobs
        WHERE id IN (
            SELECT id FROM (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY dataset_id, range_key
                        ORDER BY finished_at DESC NULLS LAST,
                                 started_at DESC NULLS LAST,
                                 id DESC
                    ) AS rn
                FROM summary_jobs
            ) ranked
            WHERE ranked.rn > 1
        )
    """
    if dry_run:
        LOGGER.info("Dry run enabled – no rows deleted.")
        return
    with engine.begin() as connection:
        result = connection.execute(text(delete_sql))
        LOGGER.info("Removed %d duplicate job(s).", result.rowcount or 0)


def _reset_tables(engine, dry_run: bool) -> None:
    LOGGER.warning("Resetting summary tables (exports, reports, jobs).")
    statements = [
        "DELETE FROM summary_exports",
        "DELETE FROM summary_reports",
        "DELETE FROM summary_jobs",
    ]
    if dry_run:
        LOGGER.info("Dry run enabled – no tables truncated.")
        return
    with engine.begin() as connection:
        for stmt in statements:
            connection.execute(text(stmt))
    LOGGER.info("Summary tables cleared.")


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    database_url = _load_database_url()
    engine = create_engine(database_url, future=True)

    if args.reset:
        _reset_tables(engine, args.dry_run)
    else:
        _prune_duplicates(engine, args.dry_run)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to manage summary tables: %s", exc)
        sys.exit(1)
