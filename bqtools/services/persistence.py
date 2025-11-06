from __future__ import annotations

import atexit
import logging
import os
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any, Iterator, Literal

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
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

PersistenceMode = Literal["DB", "CSV"]


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
    intraday_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("FALSE"),
    )
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
    intraday_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("FALSE"),
    )

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
    intraday_fallback: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("FALSE"),
    )
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


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


class PersistenceService:
    """Controls whether we persist to PostgreSQL or fall back to CSV mode."""

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("summary_app.persistence")
        self._engine: Engine | None = None
        self._session_factory: sessionmaker[Session] | None = None
        self._cloud_sql_connector: Any | None = None
        self._cloud_sql_connector_registered = False

    @property
    def mode(self) -> PersistenceMode:
        raw_value = os.environ.get("USE_PERSISTENCY_MODE", "DB") or "DB"
        normalized = raw_value.strip().upper()
        return "CSV" if normalized == "CSV" else "DB"

    def is_database_mode(self) -> bool:
        return self.mode == "DB"

    def _database_url(self) -> str:
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError(
                "DATABASE_URL environment variable is required for PostgreSQL persistence."
            )
        if url.startswith("postgresql+asyncpg://"):
            self._logger.warning(
                "DATABASE_URL specified asyncpg driver; switching to psycopg driver."
            )
            url = "postgresql+psycopg://" + url[len("postgresql+asyncpg://") :]
        elif url.startswith("postgresql://"):
            url = "postgresql+psycopg://" + url[len("postgresql://") :]
        return url

    def configure_engine(self, *, echo: bool = False) -> Engine:
        if not self.is_database_mode():
            raise RuntimeError("Engine configuration requested while CSV persistence is active.")
        if self._engine is None:
            if self._should_use_cloud_sql_connector():
                self._engine = self._create_connector_engine(echo=echo)
            else:
                database_url = self._database_url()
                self._engine = create_engine(database_url, echo=echo, future=True)
            self._session_factory = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)
            Base.metadata.create_all(bind=self._engine)
            self._ensure_schema_updates(self._engine)
            self._logger.info("Configured PostgreSQL engine and ensured metadata exists.")
        return self._engine

    def has_configured_engine(self) -> bool:
        return self._engine is not None

    def get_session(self) -> Session:
        if not self.is_database_mode():
            raise RuntimeError("Database session requested while CSV persistence is active.")
        self.configure_engine()
        if self._session_factory is None:
            raise RuntimeError("Database session factory is not configured.")
        session = self._session_factory()
        self._logger.debug("Opened new database session.")
        return session

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.get_session()
        try:
            yield session
            session.commit()
            self._logger.debug("Committed session in session_scope.")
        except Exception:
            session.rollback()
            self._logger.exception("Rolling back session due to exception.")
            raise
        finally:
            session.close()
            self._logger.debug("Closed session in session_scope.")

    def _ensure_schema_updates(self, engine: Engine) -> None:
        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names())
        with engine.begin() as connection:
            if "datasets" in existing_tables:
                dataset_columns = {column["name"] for column in inspector.get_columns("datasets")}
                if "intraday_active" not in dataset_columns:
                    self._logger.info("Applying schema update: adding datasets.intraday_active column.")
                    connection.execute(
                        text("ALTER TABLE datasets ADD COLUMN intraday_active BOOLEAN NOT NULL DEFAULT FALSE")
                    )
            if "summary_jobs" in existing_tables:
                job_columns = {column["name"] for column in inspector.get_columns("summary_jobs")}
                if "intraday_active" not in job_columns:
                    self._logger.info("Applying schema update: adding summary_jobs.intraday_active column.")
                    connection.execute(
                        text("ALTER TABLE summary_jobs ADD COLUMN intraday_active BOOLEAN NOT NULL DEFAULT FALSE")
                    )
            if "summary_exports" in existing_tables:
                export_columns = {column["name"] for column in inspector.get_columns("summary_exports")}
                if "intraday_fallback" not in export_columns:
                    self._logger.info("Applying schema update: adding summary_exports.intraday_fallback column.")
                    connection.execute(
                        text("ALTER TABLE summary_exports ADD COLUMN intraday_fallback BOOLEAN NOT NULL DEFAULT FALSE")
                    )

    def _should_use_cloud_sql_connector(self) -> bool:
        default = bool(os.environ.get("CLOUD_SQL_INSTANCE_CONNECTION_NAME"))
        return _env_flag("CLOUD_SQL_USE_CONNECTOR", default=default)

    def _ensure_cloud_sql_connector(self):
        if self._cloud_sql_connector is None:
            try:
                from google.cloud.sql.connector import Connector  # type: ignore
            except ImportError as exc:  # pragma: no cover - import guard
                raise RuntimeError(
                    "google-cloud-sql-python-connector is required when Cloud SQL connectivity is enabled."
                ) from exc
            self._cloud_sql_connector = Connector()
        if not self._cloud_sql_connector_registered:
            atexit.register(self._cloud_sql_connector.close)
            self._cloud_sql_connector_registered = True
        return self._cloud_sql_connector

    def _cloud_sql_config(self) -> dict[str, Any]:
        try:
            from google.cloud.sql.connector import IPTypes  # type: ignore
        except ImportError as exc:  # pragma: no cover - import guard
            raise RuntimeError(
                "google-cloud-sql-python-connector is required when Cloud SQL connectivity is enabled."
            ) from exc

        config = {
            "instance_connection_name": os.environ.get("CLOUD_SQL_INSTANCE_CONNECTION_NAME"),
            "db_user": os.environ.get("CLOUD_SQL_DB_USER"),
            "db_password": os.environ.get("CLOUD_SQL_DB_PASSWORD"),
            "db_name": os.environ.get("CLOUD_SQL_DB_NAME"),
        }
        missing = [key for key, value in config.items() if not value]
        if missing:
            raise RuntimeError(
                "Missing Cloud SQL connector environment variables: " + ", ".join(sorted(missing))
            )

        ip_pref = (os.environ.get("CLOUD_SQL_IP_TYPE", "PUBLIC") or "PUBLIC").strip().upper()
        ip_type = IPTypes.PRIVATE if ip_pref == "PRIVATE" else IPTypes.PUBLIC

        driver = (os.environ.get("CLOUD_SQL_CONNECTOR_DRIVER") or "psycopg").strip()
        if not driver:
            driver = "psycopg"

        return {
            **config,
            "ip_type": ip_type,
            "driver": driver,
        }

    def _create_connector_engine(self, *, echo: bool) -> Engine:
        settings = self._cloud_sql_config()
        connector = self._ensure_cloud_sql_connector()
        driver = settings["driver"]
        sqlalchemy_url = f"postgresql+{driver}://"

        def getconn():
            return connector.connect(
                settings["instance_connection_name"],
                driver,
                user=settings["db_user"],
                password=settings["db_password"],
                db=settings["db_name"],
                ip_type=settings["ip_type"],
            )

        self._logger.info(
            "Connecting to Cloud SQL instance %s via %s driver.",
            settings["instance_connection_name"],
            driver,
        )
        return create_engine(
            sqlalchemy_url,
            creator=getconn,
            pool_pre_ping=True,
            echo=echo,
            future=True,
        )


__all__ = [
    "Base",
    "Dataset",
    "PersistenceMode",
    "PersistenceService",
    "SummaryExport",
    "SummaryJob",
    "SummaryReport",
]
