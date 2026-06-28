#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROJECT_ID="${PROJECT_ID:-${BQ_PROJECT:-}}"
TARGET_DATASET="${TARGET_DATASET:-${BQ_DATASET:-waca_core_output}}"
SAMPLE_DATASET="${SAMPLE_GA4_DATASET:-waca_core_sample_ga4}"
LOCATION="${BQ_LOCATION:-asia-northeast1}"

required_files=(
  "README.md"
  "INSTALL.md"
  "CONTRIBUTING.md"
  "SECURITY.md"
  "LICENSE"
  ".env.example"
  "src/run_waca_core_batch.sql"
  "src/cloud_functions/waca-core-daily-batch/main.py"
  "src/cloud_functions/waca-core-daily-batch/requirements.txt"
  "samples/bigquery/create_anonymous_ga4_sample.sql"
  "scripts/create_sample_dataset.sh"
)

for file in "${required_files[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "ERROR: required file is missing: $file" >&2
    exit 1
  fi
done

forbidden_paths=(
  "docs"
  "tests"
  "training"
  "metrics"
)

for path in "${forbidden_paths[@]}"; do
  if [[ -e "$path" ]]; then
    echo "ERROR: public install repository should not contain: $path" >&2
    exit 1
  fi
done

if find . -type d \( -name '__pycache__' -o -name '.pytest_cache' -o -name '.mypy_cache' \) -print -quit | grep -q .; then
  echo "ERROR: cache directories must not be included in the public install repository." >&2
  exit 1
fi

echo "Static public-install checks passed."

if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "your-gcp-project-id" ]]; then
  cat <<'EOF'

Set PROJECT_ID (or BQ_PROJECT) to your Google Cloud project to also dry-run the
anonymous sample dataset SQL and the WACA core stored procedure SQL:

  PROJECT_ID=your-gcp-project-id DRY_RUN=1 bash scripts/install_smoke_check.sh
EOF
  exit 0
fi

if ! command -v bq >/dev/null 2>&1; then
  echo "bq command not found. Static checks passed, but BigQuery dry-run was skipped." >&2
  echo "Install and authenticate the Google Cloud SDK, then rerun this script." >&2
  exit 0
fi

echo "Dry-running anonymous GA4 sample dataset SQL..."
PROJECT_ID="$PROJECT_ID" \
SAMPLE_GA4_DATASET="$SAMPLE_DATASET" \
BQ_LOCATION="$LOCATION" \
DRY_RUN=1 \
scripts/create_sample_dataset.sh

tmp_sql="$(mktemp)"
trap 'rm -f "$tmp_sql"' EXIT

perl -pe \
  "s/your-gcp-project-id/${PROJECT_ID}/g; s/your-dataset-id/${TARGET_DATASET}/g" \
  src/run_waca_core_batch.sql > "$tmp_sql"

# The stored procedure is created as `PROJECT.TARGET_DATASET.run_waca_core_batch`,
# so BigQuery needs the output dataset to exist before it can validate the SQL.
# Create it if missing. This is the same output dataset used by the real install.
if ! bq show --project_id="$PROJECT_ID" "${PROJECT_ID}:${TARGET_DATASET}" >/dev/null 2>&1; then
  echo "Output dataset ${PROJECT_ID}:${TARGET_DATASET} does not exist. Creating it (required to validate the stored procedure)..."
  bq --location="$LOCATION" mk --dataset "${PROJECT_ID}:${TARGET_DATASET}"
fi

echo "Dry-running WACA core stored procedure SQL..."
bq query \
  --location="$LOCATION" \
  --dry_run \
  --use_legacy_sql=false \
  < "$tmp_sql"

cat <<EOF
WACA core install smoke check passed.

Verified:
- required public install files
- no non-install directories
- anonymous sample dataset SQL dry-run
- run_waca_core_batch stored procedure SQL dry-run

This smoke check does not execute CALL run_waca_core_batch. Run the full batch
only after reviewing scanned bytes, date range, and dataset names.
EOF
