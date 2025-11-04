# bigquery-test

BigQuery exploration toolkit with reusable services shared by the CLI scripts and the Flask UI.

## Project layout

- `bqtools/` – shared library code. It contains the authentication helpers, a `BigQueryService` for core table/query operations, and a `DatasetSummaryService` that powers both the CLI and the web UI.
- `bigquery.py` – thin CLI wrapper around `BigQueryService` for quick connectivity checks, previews, and CSV exports.
- `summarize_dataset.py` – CLI wrapper around `DatasetSummaryService` that renders the human-readable dataset report.
- `app.py` – Flask application that exposes the same summary data through a browser UI.
- `bqtools/persistence/` – adapters that persist query results to relational databases or vector stores for downstream analytics.

Setup (macOS / zsh):

1. Create and activate a virtual environment and install dependencies:

```bash
./setup_venv.sh
source .venv/bin/activate
```

2. Provide credentials. Either place `oauth_credentials.json` in the project root or point to another file:

```bash
export BIGQUERY_CREDENTIALS_FILE=/absolute/path/to/your/key.json
```

3. Run the connectivity script:

```bash
python3 bigquery.py
```

To fetch only the row count for a table:

```bash
python3 bigquery.py --table DATASET.TABLE --row-count
```

You can still preview rows with `--table` alone (defaults to five rows, tweak with `--max-rows`).

Common flags include `--project`, `--dataset`, `--location`, and repeated `--scope` entries when you need to override the default BigQuery scope.

To export data to CSV, combine `--csv` with either `--table` or `--dataset`:

```bash
# Write a single table to /tmp/events.csv
python3 bigquery.py --table DATASET.TABLE --csv /tmp/events.csv

# Export every table in a dataset to the ./exports directory
python3 bigquery.py --dataset PROJECT_ID.DATASET --csv ./exports

# Let the script pick the output path automatically
python3 bigquery.py --table DATASET.TABLE --csv
```

When you omit the output argument, the script saves tables as `PROJECT_DATASET_TABLE.csv` (or dataset exports inside a `PROJECT_DATASET_csv/` directory) in the current working directory.

Notes:
- For production usage prefer service-account credentials with the `google-cloud-bigquery` client.
- The shared library layer makes it straightforward to add new surfaces (database writers, chat assistants, dashboards) without duplicating BigQuery plumbing.

## Persistence adapters

You can persist `BigQueryService` results with the adapters under `bqtools.persistence`.

```python
from pathlib import Path

from sqlalchemy import create_engine

from bqtools import BigQueryService, SqlPersistenceAdapter, SqlPersistenceOptions, build_client

engine = create_engine("postgresql+psycopg://user:pass@host:5432/analytics")
client = build_client(credentials_file=Path("oauth_credentials.json"))
service = BigQueryService(client)

batch = service.preview_table_batch("dataset.table", location="EU", max_rows=1000)
sql_adapter = SqlPersistenceAdapter(engine)
sql_adapter.persist_batch(batch, options=SqlPersistenceOptions(if_exists="replace", schema="public"))
```

Vector stores follow the same pattern: supply a client with an `upsert` method and a vectoriser that turns rows into embeddings.

```python
from typing import Any

from bqtools import VectorStoreAdapter

def embed_row(row: dict[str, Any]) -> list[float]:
    return embedding_model.embed_query(" ".join(map(str, row.values())))

vector_adapter = VectorStoreAdapter(vector_client, vectoriser=embed_row)
vector_adapter.persist_batch(batch, namespace="daily_snapshot")
```

## Dataset summary helper

To generate a human-readable report for a dataset (for example `euronics-1047.analytics_308868785`) use:

```bash
python3 summarize_dataset.py --dataset euronics-1047.analytics_308868785
```

By default the script focuses on *today's* intraday table (`events_intraday_YYYYMMDD`); pass `--intraday-date 20251016` to target another day or `--all-tables` to include everything in the dataset.

The script prints:
- dataset level metadata (location, description, expiration defaults, labels)
- a compact overview of every table (row count, storage footprint, last modified)
- per-table column descriptions with lightweight statistics:
  - min/max/average/null ratios for up to six numeric/date columns
  - top values for up to four string/boolean columns using `APPROX_TOP_COUNT`

You can adjust the scope with flags such as:

- `--max-numeric-columns 10` to scan more numeric fields
- `--max-categorical-columns 8 --top-values 10` for a deeper categorical breakdown
- `--location EU` to hint the region of your dataset and reduce cross-location queries

All flags accept the same credential options as `bigquery.py`, so you can supply a service account JSON or override the project with `--project YOUR_PROJECT_ID` when needed.

Programmatic usage mirrors the CLI:

```python
from pathlib import Path

from bqtools import DatasetSummaryOptions, DatasetSummaryService, build_client

client = build_client(credentials_file=Path("oauth_credentials.json"))
service = DatasetSummaryService(client)
dataset_ref = service.resolve_dataset_ref("euronics-1047.analytics_308868785")
dataset = client.get_dataset(dataset_ref)
tables, _ = service.get_tables_for_summary(dataset_ref=dataset_ref, all_tables=True,
                                           intraday_date=None, intraday_table_prefix="events_intraday_")
summary = service.build_summary(dataset=dataset, tables=tables, options=DatasetSummaryOptions())
```

### Offline CSV summaries

Set the following environment variables to reuse the CLI for locally exported data instead of hitting BigQuery:

```
BIGQUERY_USE_CSV=true
BIGQUERY_USE_CSV_FILE=/absolute/path/to/export.csv
```

When enabled, `summarize_dataset.py` produces a GA4-style intraday digest: executive metrics (events, users, sessions, engagement), top events/pages, device and geo mixes, plus analyst highlights and next-step recommendations. The `--max-*` flags still control how many columns and top values are displayed, and the narrative mirrors the companion PDF.

### Credential path configuration

Both CLI tools (`bigquery.py`, `summarize_dataset.py`) and the web UI read the OAuth/service account JSON from:

1. The `--credentials-file` flag when provided.
2. The `BIGQUERY_CREDENTIALS_FILE` environment variable (supports `.env` files via `python-dotenv`).
3. Falling back to `oauth_credentials.json` in the project root.

Example `.env` snippet:

```
BIGQUERY_CREDENTIALS_FILE=/absolute/path/to/your/key.json
```

Run `export BIGQUERY_CREDENTIALS_FILE=/path/to/key.json` in the shell if you prefer not to use `.env`.

## Minimal web UI

For a lightweight browser view of the same summary data, install dependencies and then run:

```bash
export FLASK_APP=app.py  # optional when invoking python directly
python3 app.py
```

Open [http://127.0.0.1:5000/](http://127.0.0.1:5000/) and enter:
- the dataset ID (for example `euronics-1047.analytics_308868785`)
- optional intraday date or tick “Include all tables”
- optional overrides for credentials file, project, and region

The page displays dataset metadata, a table overview, and per-table drill-down sections rendered as HTML tables for easy sharing with non-technical stakeholders.

Daily run logs land under `var/logs/` with filenames prefixed by the UTC date (for example `2025-11-04-app.log`). Cached summary reuse and BigQuery refreshes are annotated there, making it easy to confirm when the app serves previously generated reports.

### AI Advisor bootstrap (Pinecone + OpenAI)

Add the following environment variables (via `.env` or your shell) before running the manual ingestion command or enabling the chat assistant:

```
OPENAI_API_KEY=sk-...
OPENAI_CHAT_MODEL=gpt-4o-mini
OPENAI_EMBED_MODEL=text-embedding-3-small
PINECONE_API_KEY=pc-...
PINECONE_ENVIRONMENT=gcp-starter
PINECONE_INDEX_NAME=ga4-summaries
PINECONE_NAMESPACE=ga4
```

The free Pinecone “Starter” tier supports a single index in the `gcp-starter` environment running one `s1.x1` pod (max dimension 1536, which matches `text-embedding-3-small`). Upgrades are seamless—create a larger pod-based or serverless index with the same name/namespace and re-run the ingestion script to repopulate vectors.

Once GA4 summaries exist in the PostgreSQL cache, trigger manual embedding syncs:

```bash
python3 scripts/ingest_pinecone.py --range today
python3 scripts/ingest_pinecone.py --range last7days --limit 5
python3 scripts/ingest_pinecone.py --range all --dry-run  # preview without upsert
```

The script loads completed summary jobs, embeds the full summary JSON payload, and upserts the vectors (with metadata) into the configured Pinecone namespace.

### HTTP API

The Flask app also exposes a JSON API under `/api/summary`, powered by the same service layer. POST a payload to retrieve structured data (and optional pre-rendered text) suitable for chat/LLM integrations or dashboards:

```bash
curl -X POST http://127.0.0.1:5000/api/summary \
  -H "Content-Type: application/json" \
  -d '{
        "dataset": "euronics-1047.analytics_308868785",
        "include_text": true,
        "all_tables": true
      }'
```

Response snippet:

```json
{
  "summary": {"dataset": {"dataset_id": "analytics_308868785", ...}},
  "summary_text": "Dataset: euronics-1047.analytics_308868785\n...",
  "filter_note": null
}
```

Override scopes, credentials, or options like `intraday_date`, `max_numeric_columns`, and `max_top_values` by supplying them in the JSON body.
