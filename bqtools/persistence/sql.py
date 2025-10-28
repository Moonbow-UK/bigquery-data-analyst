from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from sqlalchemy import (
    Column,
    inspect,
    MetaData,
    Table,
    exc as sa_exc,
    insert,
    types as sa_types,
)
from sqlalchemy.engine import Engine
from sqlalchemy.engine import create_engine

from .base import TableBatch


@dataclass(slots=True)
class SqlPersistenceOptions:
    if_exists: str = "replace"  # replace|append|skip
    schema: str | None = None


class SqlPersistenceAdapter:
    """
    Persist tabular data into MySQL/PostgreSQL using SQLAlchemy Core.

    Instantiate with either an SQLAlchemy Engine or a database URL. Column types
    are inferred from Python values when BigQuery schema information is absent.
    """

    def __init__(self, engine: Engine | str, *, metadata: MetaData | None = None) -> None:
        self._engine = create_engine(engine) if isinstance(engine, str) else engine
        self._metadata = metadata or MetaData()

    def persist_batch(
        self,
        batch: TableBatch,
        *,
        table_name: str | None = None,
        options: SqlPersistenceOptions | None = None,
    ) -> None:
        table_name = table_name or self._normalise_table_name(batch.table_id)
        options = options or SqlPersistenceOptions()
        if options.if_exists not in {"replace", "append", "skip"}:
            raise ValueError("if_exists must be one of {'replace', 'append', 'skip'}")

        with self._engine.begin() as connection:
            table, created = self._prepare_table(
                table_name=table_name,
                headers=batch.headers,
                schema=batch.schema,
                options=options,
                connection_engine=connection.engine,
            )

            if not created and options.if_exists == "skip":
                return

            if options.if_exists == "replace" and not created:
                connection.execute(table.delete())

            dict_rows = list(batch.iter_dicts())
            if not dict_rows:
                return

            connection.execute(insert(table), dict_rows)

    def _prepare_table(
        self,
        *,
        table_name: str,
        headers: Sequence[str],
        schema: Sequence[Any] | None,
        options: SqlPersistenceOptions,
        connection_engine: Engine,
    ) -> tuple[Table, bool]:
        metadata = self._metadata
        table_key = f"{options.schema}.{table_name}" if options.schema else table_name
        existing = metadata.tables.get(table_key)
        if existing is not None:
            return existing, False

        columns = self._build_columns(headers=headers, schema=schema)
        table = Table(
            table_name,
            metadata,
            *columns,
            schema=options.schema,
        )

        metadata.bind = connection_engine

        inspector = inspect(connection_engine)
        exists = inspector.has_table(table_name, schema=options.schema)
        created = False
        try:
            if not exists:
                table.create(bind=connection_engine)
                created = True
        except sa_exc.DBAPIError as exc:
            raise RuntimeError(f"Failed to create table {table_name}: {exc}") from exc

        return table, created

    def _build_columns(
        self,
        *,
        headers: Sequence[str],
        schema: Sequence[Any] | None,
    ) -> list[Column]:
        columns: list[Column] = []
        type_hints = self._build_type_hints(schema)
        for header in headers:
            col_type = type_hints.get(header) or sa_types.String()
            columns.append(Column(self._sanitise_column_name(header), col_type))
        return columns

    def _build_type_hints(self, schema: Sequence[Any] | None) -> Mapping[str, sa_types.TypeEngine]:
        if not schema:
            return {}

        mapping: dict[str, sa_types.TypeEngine] = {}

        # Import lazily to avoid hard dependency during type checking.
        from google.cloud import bigquery  # type: ignore

        type_map: Mapping[str, sa_types.TypeEngine] = {
            "STRING": sa_types.Text(),
            "BYTES": sa_types.LargeBinary(),
            "INTEGER": sa_types.BigInteger(),
            "INT64": sa_types.BigInteger(),
            "FLOAT": sa_types.Float(),
            "FLOAT64": sa_types.Float(),
            "NUMERIC": sa_types.Numeric(),
            "BIGNUMERIC": sa_types.Numeric(),
            "BOOLEAN": sa_types.Boolean(),
            "BOOL": sa_types.Boolean(),
            "TIMESTAMP": sa_types.DateTime(timezone=True),
            "DATETIME": sa_types.DateTime(timezone=False),
            "DATE": sa_types.Date(),
            "TIME": sa_types.Time(),
        }

        for field in schema:
            if not isinstance(field, bigquery.SchemaField):
                continue
            sql_type = type_map.get(field.field_type.upper(), sa_types.Text())
            mapping[field.name] = sql_type

        return mapping

    @staticmethod
    def _normalise_table_name(table_id: str) -> str:
        cleaned = table_id.strip().strip("`")
        if "." in cleaned:
            cleaned = cleaned.split(".")[-1]
        return cleaned.replace(":", "_").replace("-", "_")

    @staticmethod
    def _sanitise_column_name(name: str) -> str:
        return name.strip().replace(" ", "_").replace("-", "_")
