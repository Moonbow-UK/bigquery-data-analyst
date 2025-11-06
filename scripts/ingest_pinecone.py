#!/usr/bin/env python3
"""Manual ingestion of GA4 summaries into Pinecone."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Iterable

from openai import OpenAI
from pinecone import Pinecone  # type: ignore[import]
from sqlalchemy import select
from sqlalchemy.orm import joinedload

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bqtools.config import load_environment
from bqtools.services.persistence import (  # noqa: E402
    Dataset,
    PersistenceService,
    SummaryJob,
    SummaryReport,
)

MAX_EMBED_TEXT_CHARS = 20000
LOGGER = logging.getLogger("pinecone_ingest")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Embed GA4 summary JSON and upsert into Pinecone."
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        help="Optional .env file to load before running (e.g., .env.prod for remote assets).",
    )
    parser.add_argument(
        "--range",
        dest="range_key",
        default="today",
        help="Summary range key to ingest (default: today, use 'all' for every range).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of summary jobs to ingest (default: 20, use 0 for no limit).",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("PINECONE_NAMESPACE", "ga4"),
        help="Pinecone namespace override.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute embeddings but skip Pinecone upsert.",
    )
    return parser.parse_args()


def _load_jobs(
    session,
    range_key: str,
    limit: int | None,
) -> list[SummaryJob]:
    stmt = (
        select(SummaryJob)
        .options(
            joinedload(SummaryJob.dataset),
            joinedload(SummaryJob.report),
        )
        .where(SummaryJob.status == "completed")
        .order_by(SummaryJob.finished_at.desc().nullslast())
    )
    if range_key.lower() != "all":
        stmt = stmt.where(SummaryJob.range_key == range_key.lower())
    if limit and limit > 0:
        stmt = stmt.limit(limit)
    jobs = list(
        session.execute(stmt).scalars().unique()
    )
    return [job for job in jobs if job.report and job.report.raw_summary_json]


def _prune_summary_payload(summary_payload: object) -> object:
    if not isinstance(summary_payload, dict):
        return summary_payload
    pruned: dict[str, object] = {}
    for key, value in summary_payload.items():
        if key == "csv_sections" and isinstance(value, list):
            trimmed_sections = []
            for section in value[:5]:
                if isinstance(section, dict):
                    trimmed_section = dict(section)
                    rows = trimmed_section.get("rows")
                    if isinstance(rows, list):
                        trimmed_section["rows"] = rows[:5]
                    trimmed_sections.append(trimmed_section)
            pruned[key] = trimmed_sections
        elif key == "tables" and isinstance(value, list):
            pruned[key] = value[:10]
        elif key == "table_overview" and isinstance(value, list):
            pruned[key] = value[:10]
        else:
            pruned[key] = value
    return pruned


def _serialise_summary(report: SummaryReport, job: SummaryJob, dataset: Dataset | None) -> str:
    summary_payload = report.raw_summary_json
    if isinstance(summary_payload, str):
        try:
            summary_payload = json.loads(summary_payload)
        except json.JSONDecodeError:
            LOGGER.warning("Stored summary JSON malformed for job %s", job.id)
            summary_payload = {"raw_summary": summary_payload}
    summary_payload = _prune_summary_payload(summary_payload)

    body = {
        "dataset": (
            f"{dataset.project_id}.{dataset.dataset_id}"
            if dataset
            else "unknown"
        ),
        "range": job.range_key,
        "filter_note": job.filter_note,
        "intraday_active": job.intraday_active,
        "summary": summary_payload,
    }
    serialised = json.dumps(body, sort_keys=True)
    if len(serialised) > MAX_EMBED_TEXT_CHARS:
        LOGGER.warning(
            "Embedding text for job %s exceeds limit (%d chars). Truncating to %d chars.",
            job.id,
            len(serialised),
            MAX_EMBED_TEXT_CHARS,
        )
        serialised = serialised[:MAX_EMBED_TEXT_CHARS]
    return serialised


def _embed_text(client: OpenAI, model: str, text: str) -> list[float]:
    response = client.embeddings.create(model=model, input=text)
    return response.data[0].embedding


def _batched(iterable: Iterable, size: int) -> Iterable[list]:
    batch: list = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def main() -> None:
    args = _parse_args()
    load_environment(args.env_file)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )

    pinecone_api_key = os.environ.get("PINECONE_API_KEY")
    pinecone_env = os.environ.get("PINECONE_ENVIRONMENT")
    pinecone_index_name = os.environ.get("PINECONE_INDEX_NAME")
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    embed_model = os.environ.get("OPENAI_EMBED_MODEL", "text-embedding-3-small")

    if not pinecone_api_key or not pinecone_env or not pinecone_index_name:
        LOGGER.error("Pinecone configuration missing. Check PINECONE_* environment variables.")
        sys.exit(1)
    if not openai_api_key:
        LOGGER.error("OPENAI_API_KEY not provided.")
        sys.exit(1)

    persistence_service = PersistenceService(logger=LOGGER.getChild("persistence"))
    try:
        session = persistence_service.get_session()
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Unable to obtain database session: %s", exc)
        sys.exit(1)

    try:
        jobs = _load_jobs(session, args.range_key, args.limit)
        if not jobs:
            LOGGER.info("No completed summary jobs found for range=%s", args.range_key)
            return

        LOGGER.info("Preparing embeddings for %d job(s).", len(jobs))
        openai_client = OpenAI(api_key=openai_api_key)

        vectors: list[dict[str, object]] = []
        for job in jobs:
            report = job.report
            dataset = job.dataset
            text_payload = _serialise_summary(report, job, dataset)
            embedding = _embed_text(openai_client, embed_model, text_payload)
            metadata = {
                "job_id": str(job.id),
                "dataset": (
                    f"{dataset.project_id}.{dataset.dataset_id}"
                    if dataset
                    else None
                ),
                "range_key": job.range_key,
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            }
            vectors.append(
                {
                    "id": str(job.id),
                    "values": embedding,
                    "metadata": metadata,
                }
            )

        if args.dry_run:
            LOGGER.info("Dry run enabled – skipping Pinecone upsert.")
            return

        pinecone_client = Pinecone(api_key=pinecone_api_key, environment=pinecone_env)
        existing_indexes = pinecone_client.list_indexes().names()
        if pinecone_index_name not in existing_indexes:
            LOGGER.error(
                "Pinecone index '%s' not found. Create it first (dimension should match %s embeddings).",
                pinecone_index_name,
                embed_model,
            )
            sys.exit(1)

        index = pinecone_client.Index(pinecone_index_name)
        total = 0
        for batch in _batched(vectors, 50):
            index.upsert(vectors=batch, namespace=args.namespace)
            total += len(batch)
        LOGGER.info("Upserted %d vector(s) into Pinecone namespace '%s'.", total, args.namespace)
    finally:
        session.close()
        LOGGER.debug("Database session closed.")


if __name__ == "__main__":
    main()
