# Install WACA core

This guide installs WACA core from Git and runs it against either:

1. Your own GA4 BigQuery export dataset, or
2. The anonymous sample GA4 dataset included in this repository.

The procedure runs in your own Google Cloud project. WACA does not receive or
store your GA4 data.

> **Important — use a dedicated, empty output dataset.**
> The examples below call WACA core with `force_reset=TRUE`. On a force reset,
> the procedure drops every table in the output dataset that it does not manage.
> If `TARGET_DATASET` points at a dataset that already holds your own tables,
> those tables are deleted. Always create a new, empty dataset for WACA core
> output (for example `waca_core_output`), in the same BigQuery location as your
> GA4 export.
>
> **重要 — 専用の空 dataset を使ってください。**
> 以下の手順例は `force_reset=TRUE` で WACA core を実行します。force reset 時、
> procedure は出力 dataset 内の管理外テーブルをすべて削除します。既存の自分の
> テーブルがある dataset を `TARGET_DATASET` に指定すると、それらは削除されます。
> 必ず WACA core 出力専用の空 dataset（例: `waca_core_output`）を、GA4 export と
> 同じ BigQuery location で新規作成してください。

## 1. Prepare Google Cloud and GA4

Before installing WACA core, prepare the data source:

1. Create or choose a Google Cloud project.
2. Enable BigQuery.
3. Link GA4 to BigQuery export, or use the anonymous sample dataset in this
   repository.
4. Authenticate the Google Cloud SDK:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project your-gcp-project-id
```

5. Confirm the `bq` command works:

```bash
bq ls --project_id=your-gcp-project-id
```

If you do not yet have GA4 BigQuery export enabled, you can still rehearse the
install with the anonymous sample dataset.

## 2. Clone and Configure

```bash
git clone https://github.com/wacasg/waca-core-public.git
cd waca-core-public
cp .env.example .env
```

Open `.env` and replace the placeholders:

| Variable | Meaning |
|---|---|
| `BQ_PROJECT` / `PROJECT_ID` | Your Google Cloud project ID. |
| `BQ_LOCATION` | BigQuery location, such as `asia-northeast1` or `US`. |
| `BQ_DATASET` / `TARGET_DATASET` | Output dataset for WACA core tables. |
| `GA4_BQ_DATASET` / `SOURCE_DATASET` | Input GA4 export dataset, such as `analytics_123456789`. |
| `SAMPLE_GA4_DATASET` | Anonymous sample dataset name used for rehearsal. |
| `PROCEDURE_NAME` | Keep `run_waca_core_batch` unless you intentionally rename it. |
| `CLIENT_ID` | A label stored in batch logs, such as `example-client` or your site name. |

Do not commit `.env`. It may contain project IDs, credential paths, or other
local settings.

## 3. Run Static Smoke Check

Run the smoke check without Google Cloud access first:

```bash
bash scripts/install_smoke_check.sh
```

This checks the public install file set. It does not connect to BigQuery unless
you set `PROJECT_ID`.

## 4. Dry-Run BigQuery SQL

If you have the Google Cloud SDK installed and authenticated, run:

```bash
PROJECT_ID=your-gcp-project-id DRY_RUN=1 bash scripts/install_smoke_check.sh
```

This dry-runs:

1. The anonymous sample GA4 dataset SQL.
2. The WACA core stored procedure SQL.

Dry-run validates SQL syntax and BigQuery references without creating tables or
scanning data. If the output dataset does not exist yet, the script creates the
empty output dataset first, because BigQuery needs it to validate the stored
procedure. No tables or rows are created during the dry-run.

## 5. Create the Anonymous Sample Dataset

Use this when you want a safe install rehearsal without real GA4 customer data.

```bash
PROJECT_ID=your-gcp-project-id bash scripts/create_sample_dataset.sh
```

By default this creates:

```text
your-gcp-project-id.waca_core_sample_ga4.events_20260501
your-gcp-project-id.waca_core_sample_ga4.events_20260502
```

The sample contains synthetic users, pages, events, and item data only.

## 6. Register the Stored Procedure

Create the output dataset and register the procedure:

```bash
PROJECT_ID=your-gcp-project-id
TARGET_DATASET=waca_core_output
LOCATION=asia-northeast1

bq --location="${LOCATION}" mk --dataset "${PROJECT_ID}:${TARGET_DATASET}"

perl -pe "s/your-gcp-project-id/${PROJECT_ID}/g; s/your-dataset-id/${TARGET_DATASET}/g" \
  src/run_waca_core_batch.sql \
  | bq query --location="${LOCATION}" --use_legacy_sql=false
```

If the dataset already exists, the `bq mk --dataset` command may report that it
already exists. That is fine.

## 7. Run WACA core on the Sample Dataset

After registering the procedure, call it with the sample input dataset:

```bash
PROJECT_ID=your-gcp-project-id
TARGET_DATASET=waca_core_output
SOURCE_DATASET=waca_core_sample_ga4
LOCATION=asia-northeast1

bq query --location="${LOCATION}" --use_legacy_sql=false "
CALL \`${PROJECT_ID}.${TARGET_DATASET}.run_waca_core_batch\`(
  '20260501',
  '20260502',
  '${PROJECT_ID}',
  '${TARGET_DATASET}',
  '${SOURCE_DATASET}',
  TRUE,
  FALSE,
  'example-client'
)"
```

When the run finishes, inspect the output tables:

```bash
bq query --location="${LOCATION}" --use_legacy_sql=false "
SELECT COUNT(*) AS row_count FROM \`${PROJECT_ID}.${TARGET_DATASET}.micro_user_table\`
"

bq query --location="${LOCATION}" --use_legacy_sql=false "
SELECT COUNT(*) AS row_count FROM \`${PROJECT_ID}.${TARGET_DATASET}.micro_items_table\`
"
```

## 8. Run WACA core on Your GA4 Export

Change only the source dataset and date range:

```bash
PROJECT_ID=your-gcp-project-id
TARGET_DATASET=waca_core_output
SOURCE_DATASET=analytics_123456789
LOCATION=asia-northeast1

bq query --location="${LOCATION}" --use_legacy_sql=false "
CALL \`${PROJECT_ID}.${TARGET_DATASET}.run_waca_core_batch\`(
  '20260501',
  '20260531',
  '${PROJECT_ID}',
  '${TARGET_DATASET}',
  '${SOURCE_DATASET}',
  TRUE,
  FALSE,
  'your-client-id'
)"
```

Use a small date range first. Review BigQuery scanned bytes and cost before
running long ranges.

> **Cost note.** WACA core rebuilds a 180-day user-mapping table on every run,
> so each run scans up to ~180 days of `events_*` and `pseudonymous_users_*`.
> Date filters prevent full-table scans, but on large properties this is the
> main cost driver. Use `DRY_RUN=1` to check scanned bytes before long runs.
>
> **コスト注意.** WACA core は毎回 180 日分の user-mapping table を再構築するため、
> 各実行で最大約 180 日分の `events_*` / `pseudonymous_users_*` をスキャンします。
> 日付フィルタにより全件スキャンにはなりませんが、大規模プロパティでは主な
> コスト要因です。長期間の実行前に `DRY_RUN=1` で scanned bytes を確認してください。

## 9. Optional Daily Wrapper

The folder `src/cloud_functions/waca-core-daily-batch/` contains an optional
Python wrapper for scheduled execution. It is not required for the first install.

Install its dependencies only if you plan to run the wrapper:

```bash
cd src/cloud_functions/waca-core-daily-batch
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

The wrapper reads environment variables such as `PROJECT_ID`, `TARGET_DATASET`,
`SOURCE_DATASET`, `PROCEDURE_NAME`, and `CLIENT_ID`.

## Troubleshooting

| Symptom | Check |
|---|---|
| `Repository not found` | Confirm the repository URL and your GitHub access. |
| `bq: command not found` | Install the Google Cloud SDK and confirm `bq --version`. |
| `Access Denied` | Confirm your account has BigQuery Job User and dataset permissions. |
| `Not found: Dataset ...` | Confirm `PROJECT_ID`, `TARGET_DATASET`, and `SOURCE_DATASET`. |
| No GA4 data appears | Confirm your GA4 BigQuery export has `events_*` tables for the date range. |
| `pseudonymous_users_*` is missing | WACA core can run without GA4 user-data export tables; user identifiers will remain anonymous where GA4 does not provide them. |

Do not use real customer data as sample data. Use the anonymous sample first,
then switch to your own GA4 export after the install path is clear.

## 日本語

この手順は、Git から WACA core を取得し、次のどちらかに対して実行するためのものです。

1. 利用者自身の GA4 BigQuery export dataset
2. この repository に含まれる匿名 sample GA4 dataset

WACA core は利用者自身の Google Cloud project 内で動きます。WACA が GA4 data を
受け取ったり保存したりするものではありません。

### 1. Google Cloud と GA4 を準備する

install の前に、入力元となる data を準備します。

1. Google Cloud project を作成または選択します。
2. BigQuery を有効にします。
3. GA4 と BigQuery export を連携します。まだ連携していない場合は、この repository
   の匿名 sample dataset で rehearsal できます。
4. Google Cloud SDK で認証します。

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project your-gcp-project-id
```

5. `bq` command が動くことを確認します。

```bash
bq ls --project_id=your-gcp-project-id
```

### 2. clone して設定する

```bash
git clone https://github.com/wacasg/waca-core-public.git
cd waca-core-public
cp .env.example .env
```

`.env` を開き、自分の値に置き換えます。

| 変数 | 意味 |
|---|---|
| `BQ_PROJECT` / `PROJECT_ID` | 自分の Google Cloud project ID。 |
| `BQ_LOCATION` | BigQuery location。例: `asia-northeast1` または `US`。 |
| `BQ_DATASET` / `TARGET_DATASET` | WACA core の出力先 dataset。 |
| `GA4_BQ_DATASET` / `SOURCE_DATASET` | 入力元 GA4 export dataset。例: `analytics_123456789`。 |
| `SAMPLE_GA4_DATASET` | rehearsal 用の匿名 sample dataset 名。 |
| `PROCEDURE_NAME` | 通常は `run_waca_core_batch` のまま使います。 |
| `CLIENT_ID` | batch log に残す label。例: `example-client` や site 名。 |

`.env` は commit しないでください。project ID、credential path、local setting が
含まれる可能性があります。

### 3. static smoke check を実行する

まず Google Cloud に接続しない check を実行します。

```bash
bash scripts/install_smoke_check.sh
```

これは公開 install 用のファイル構成を確認するものです。`PROJECT_ID` を指定しない限り、
BigQuery には接続しません。

### 4. BigQuery SQL を dry-run する

Google Cloud SDK が入り、認証済みであれば次を実行します。

```bash
PROJECT_ID=your-gcp-project-id DRY_RUN=1 bash scripts/install_smoke_check.sh
```

これにより、匿名 sample dataset SQL と WACA core stored procedure SQL を dry-run
します。dry-run は構文と参照を確認するためのもので、table や行 data は作成しません。
出力 dataset がまだ存在しない場合は、stored procedure の検証に必要なため、空の出力
dataset だけを先に作成します。

### 5. 匿名 sample dataset を作成する

実データを使わずに install rehearsal する場合は次を実行します。

```bash
PROJECT_ID=your-gcp-project-id bash scripts/create_sample_dataset.sh
```

標準では次の table が作成されます。

```text
your-gcp-project-id.waca_core_sample_ga4.events_20260501
your-gcp-project-id.waca_core_sample_ga4.events_20260502
```

sample には匿名の synthetic user、page、event、item data だけが含まれます。

### 6. stored procedure を登録する

出力 dataset を作り、procedure を登録します。

```bash
PROJECT_ID=your-gcp-project-id
TARGET_DATASET=waca_core_output
LOCATION=asia-northeast1

bq --location="${LOCATION}" mk --dataset "${PROJECT_ID}:${TARGET_DATASET}"

perl -pe "s/your-gcp-project-id/${PROJECT_ID}/g; s/your-dataset-id/${TARGET_DATASET}/g" \
  src/run_waca_core_batch.sql \
  | bq query --location="${LOCATION}" --use_legacy_sql=false
```

dataset がすでに存在する場合、`bq mk --dataset` は exists error を出すことがあります。
その場合は次に進んでかまいません。

### 7. sample dataset に対して実行する

procedure 登録後、sample input dataset に対して実行します。

```bash
PROJECT_ID=your-gcp-project-id
TARGET_DATASET=waca_core_output
SOURCE_DATASET=waca_core_sample_ga4
LOCATION=asia-northeast1

bq query --location="${LOCATION}" --use_legacy_sql=false "
CALL \`${PROJECT_ID}.${TARGET_DATASET}.run_waca_core_batch\`(
  '20260501',
  '20260502',
  '${PROJECT_ID}',
  '${TARGET_DATASET}',
  '${SOURCE_DATASET}',
  TRUE,
  FALSE,
  'example-client'
)"
```

完了後、出力 table を確認します。

```bash
bq query --location="${LOCATION}" --use_legacy_sql=false "
SELECT COUNT(*) AS row_count FROM \`${PROJECT_ID}.${TARGET_DATASET}.micro_user_table\`
"
```

### 8. 自分の GA4 export に対して実行する

sample で動作確認できたら、source dataset と date range を自分の GA4 export に
置き換えます。

```bash
PROJECT_ID=your-gcp-project-id
TARGET_DATASET=waca_core_output
SOURCE_DATASET=analytics_123456789
LOCATION=asia-northeast1

bq query --location="${LOCATION}" --use_legacy_sql=false "
CALL \`${PROJECT_ID}.${TARGET_DATASET}.run_waca_core_batch\`(
  '20260501',
  '20260531',
  '${PROJECT_ID}',
  '${TARGET_DATASET}',
  '${SOURCE_DATASET}',
  TRUE,
  FALSE,
  'your-client-id'
)"
```

最初は短い date range で実行してください。長い期間で実行する前に、BigQuery の
scanned bytes と cost を確認してください。

### 9. optional daily wrapper

`src/cloud_functions/waca-core-daily-batch/` は日次実行したい場合の optional Python
wrapper です。最初の install には不要です。

使う場合だけ依存関係を install します。

```bash
cd src/cloud_functions/waca-core-daily-batch
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

wrapper は `PROJECT_ID`、`TARGET_DATASET`、`SOURCE_DATASET`、`PROCEDURE_NAME`、
`CLIENT_ID` などの environment variables を読みます。

### よくある失敗

| 症状 | 確認すること |
|---|---|
| `Repository not found` | repository URL と GitHub access を確認してください。 |
| `bq: command not found` | Google Cloud SDK を install し、`bq --version` を確認してください。 |
| `Access Denied` | BigQuery Job User や dataset 権限を確認してください。 |
| `Not found: Dataset ...` | `PROJECT_ID`、`TARGET_DATASET`、`SOURCE_DATASET` を確認してください。 |
| GA4 data が出ない | 指定 date range に `events_*` table があるか確認してください。 |
| `pseudonymous_users_*` が無い | GA4 user-data export table が無くても実行できます。その場合、GA4 が提供しない user identifier は匿名のままになります。 |

sample data として実顧客 data を使わないでください。まず匿名 sample で install path を
確認し、その後に自分の GA4 export に切り替えてください。
