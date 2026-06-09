#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-${BQ_PROJECT:-}}"
DATASET_ID="${SAMPLE_GA4_DATASET:-waca_core_sample_ga4}"
LOCATION="${BQ_LOCATION:-asia-northeast1}"
DRY_RUN="${DRY_RUN:-0}"

if [[ -z "$PROJECT_ID" ]]; then
  echo "PROJECT_ID or BQ_PROJECT is required." >&2
  echo "Example: PROJECT_ID=your-gcp-project-id scripts/create_sample_dataset.sh" >&2
  exit 1
fi

if ! command -v bq >/dev/null 2>&1; then
  echo "bq command not found. Install and authenticate the Google Cloud SDK first." >&2
  exit 1
fi

args=(
  query
  "--location=${LOCATION}"
  --use_legacy_sql=false
  "--parameter=project_id:STRING:${PROJECT_ID}"
  "--parameter=dataset_id:STRING:${DATASET_ID}"
  "--parameter=location:STRING:${LOCATION}"
)

if [[ "$DRY_RUN" == "1" ]]; then
  args+=(--dry_run)
fi

bq "${args[@]}" < samples/bigquery/create_anonymous_ga4_sample.sql

if [[ "$DRY_RUN" == "1" ]]; then
  echo "Sample dataset SQL dry-run passed for ${PROJECT_ID}.${DATASET_ID}."
  exit 0
fi

bq query --location="${LOCATION}" --use_legacy_sql=false \
  "SELECT 'events_20260501' AS table_name, COUNT(*) AS row_count FROM \`${PROJECT_ID}.${DATASET_ID}.events_20260501\`
   UNION ALL
   SELECT 'events_20260502' AS table_name, COUNT(*) AS row_count FROM \`${PROJECT_ID}.${DATASET_ID}.events_20260502\`"

echo "Anonymous GA4 sample dataset ready: ${PROJECT_ID}.${DATASET_ID}"
