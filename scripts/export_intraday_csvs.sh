#!/usr/bin/env bash

set -euo pipefail

# Optional first argument sets the anchor date (YYYY-MM-DD); defaults to today (UTC).
START_DATE="${1:-$(date -u +%Y-%m-%d)}"

set -a
source .env
set +a

# Build table prefix candidates: prefer configured intraday prefix, fall back to legacy GA4 exports.
PRIMARY_PREFIX="${BIGQUERY_INTRADAY_PREFIX:-events_intraday_}"
PREFIXES=("${PRIMARY_PREFIX}")
if [[ "${PRIMARY_PREFIX}" != "events_" ]]; then
  PREFIXES+=("events_")
fi

# Sanity-check the provided start date.
if ! [[ "${START_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: START_DATE must be in YYYY-MM-DD format (received '${START_DATE}')." >&2
  exit 1
fi

mkdir -p var/exports/csv
for offset in {0..14}; do
  day=$(python3 - "${START_DATE}" "${offset}" <<'PY'
import sys
from datetime import datetime, timedelta

start = datetime.strptime(sys.argv[1], "%Y-%m-%d")
offset = int(sys.argv[2])
target = start - timedelta(days=offset)
print(target.strftime("%Y%m%d"))
PY
)
  exported=false
  for prefix in "${PREFIXES[@]}"; do
    table="${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${prefix}${day}"
    safe_prefix="${prefix%_}"
    outfile="var/exports/csv/${BIGQUERY_PROJECT_ID}_${BIGQUERY_DATASET_ID}_${safe_prefix}_${day}.csv"
    echo "Exporting ${table} → ${outfile}"
    if python3 bigquery.py --table "${table}" --csv "${outfile}" --location "${BIGQUERY_LOCATION:-EU}"; then
      exported=true
      break
    else
      rm -f "${outfile}"
      echo "Failed with prefix '${prefix}', trying next option..." >&2
    fi
  done
  if [[ "${exported}" == "false" ]]; then
    echo "ERROR: Unable to export data for ${day} using prefixes: ${PREFIXES[*]}" >&2
  fi
done
