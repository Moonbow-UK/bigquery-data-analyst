from __future__ import annotations

import logging
import os
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Iterator

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


logger = logging.getLogger("summary_app.db")


class Dataset(Base):
    __tablename__ = "datasets"
    __table_args__ = (
        UniqueConstraint("project_id", "dataset_id", name="uq_dataset_project_dataset"),
    )

    id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[str | None] = mapped_column(Text, nullable=True)
    intraday_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    credentials_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_week_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )

    summary_jobs: Mapped[list["SummaryJob"]] = relationship(
        "SummaryJob",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )


class SummaryJob(Base):
    __tablename__ = "summary_jobs"

    id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
    )
    range_key: Mapped[str] = mapped_column(Text, nullable=False)
    week_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="queued")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    progress: Mapped[float | None] = mapped_column(Float, nullable=True)
    filter_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    options_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="summary_jobs")
    exports: Mapped[list["SummaryExport"]] = relationship(
        "SummaryExport",
        back_populates="job",
        cascade="all, delete-orphan",
    )
    report: Mapped["SummaryReport"] = relationship(
        "SummaryReport",
        back_populates="job",
        cascade="all, delete-orphan",
        uselist=False,
    )


class SummaryExport(Base):
    __tablename__ = "summary_exports"

    id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("summary_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    full_table_id: Mapped[str] = mapped_column(Text, nullable=False)
    target_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    table_kind: Mapped[str | None] = mapped_column(Text, nullable=True)
    reused_cache: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    csv_row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    json_row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    csv_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    exported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["SummaryJob"] = relationship("SummaryJob", back_populates="exports")


class SummaryReport(Base):
    __tablename__ = "summary_reports"

    job_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("summary_jobs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    summary_mode: Mapped[str | None] = mapped_column(Text, nullable=True)
    csv_summary_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    total_events: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    dataset_snapshot: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    sections_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    narrative_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    raw_summary_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    job: Mapped["SummaryJob"] = relationship("SummaryJob", back_populates="report")


_ENGINE: Engine | None = None
_SESSION_FACTORY: sessionmaker[Session] | None = None


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required for PostgreSQL persistence."
        )
    if url.startswith("postgresql+asyncpg://"):
        logger.warning("DATABASE_URL specified asyncpg driver; switching to psycopg driver.")
        url = "postgresql+psycopg://" + url[len("postgresql+asyncpg://") :]
    elif url.startswith("postgresql://"):
        # Default to psycopg driver for synchronous SQLAlchemy usage.
        url = "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


def configure_engine(*, echo: bool = False) -> Engine:
    global _ENGINE, _SESSION_FACTORY
    if _ENGINE is None:
        _ENGINE = create_engine(_database_url(), echo=echo, future=True)
        _SESSION_FACTORY = sessionmaker(bind=_ENGINE, expire_on_commit=False, future=True)
        Base.metadata.create_all(bind=_ENGINE)
        logger.info("Configured PostgreSQL engine and ensured metadata exists.")
    return _ENGINE


def get_session() -> Session:
    configure_engine()
    if _SESSION_FACTORY is None:
        raise RuntimeError("Database session factory is not configured.")
    session = _SESSION_FACTORY()
    logger.debug("Opened new database session.")
    return session


def is_configured() -> bool:
    return bool(os.environ.get("DATABASE_URL"))


@contextmanager
def session_scope() -> Iterator[Session]:
    session = get_session()
    try:
        yield session
        session.commit()
        logger.debug("Committed session in session_scope.")
    except Exception:
        session.rollback()
        logger.exception("Rolling back session due to exception.")
        raise
    finally:
        session.close()
        logger.debug("Closed session in session_scope.")


__all__ = [
    "Base",
    "Dataset",
    "SummaryExport",
    "SummaryJob",
    "SummaryReport",
    "configure_engine",
    "get_session",
    "is_configured",
    "session_scope",
]
