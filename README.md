# WACA core

WACA core is an open-source BigQuery transformation package for GA4 export data.
It turns raw GA4 BigQuery tables into analysis-ready tables such as
`micro_user_table` and `micro_items_table` inside your own Google Cloud project.

WACA core does not read your GA4 screen reports. It reads the raw GA4 BigQuery
export tables that you own, creates normalized tables in a dataset that you
choose, and leaves the data in your Google Cloud environment.

## What Is Included

This public repository is intentionally small. It contains only the files needed
to install and run WACA core from Git:

| Path | Purpose |
|---|---|
| `src/run_waca_core_batch.sql` | BigQuery stored procedure that transforms GA4 export data. |
| `src/cloud_functions/waca-core-daily-batch/` | Optional Python wrapper for scheduled execution. |
| `samples/bigquery/create_anonymous_ga4_sample.sql` | Anonymous synthetic GA4 sample data for install rehearsal. |
| `scripts/create_sample_dataset.sh` | Helper script to create the sample GA4 dataset. |
| `scripts/install_smoke_check.sh` | Static and optional BigQuery dry-run checks. |
| `.env.example` | Configuration template. Copy it to `.env` and replace placeholders. |
| `INSTALL.md` | Step-by-step installation guide. |
| `CONTRIBUTING.md` | How to contribute (DCO, Apache License 2.0). |
| `SECURITY.md` | How to report security concerns privately. |
| `LICENSE` | Apache License 2.0. |

This repository is the minimal public install package for WACA core.

## Quick Start

```bash
git clone https://github.com/wacasg/waca-core.git
cd waca-core
cp .env.example .env

# Static check only. This does not connect to BigQuery.
bash scripts/install_smoke_check.sh
```

To dry-run the sample dataset SQL and the stored procedure SQL against your own
Google Cloud project:

```bash
PROJECT_ID=your-gcp-project-id DRY_RUN=1 bash scripts/install_smoke_check.sh
```

To create the anonymous sample dataset:

```bash
PROJECT_ID=your-gcp-project-id bash scripts/create_sample_dataset.sh
```

Then follow [INSTALL.md](INSTALL.md) to register and call the stored procedure.

## Data Model

WACA core creates a set of BigQuery tables for people- and session-level
analysis. The two central output tables are:

| Table | Meaning |
|---|---|
| `micro_user_table` | Event-level rows enriched with user, session, page, device, geography, traffic, and observed parameter columns. |
| `micro_items_table` | Item-level rows for ecommerce and item-related events. |

The procedure also creates helper and log tables, such as event-parameter
masters, schema history, batch execution logs, and user-mapping tables. These
tables are created in the output dataset you configure.

## BigQuery Location and Time Zone

A BigQuery query normally runs in a single location, so for a standard install
create the output dataset (for example `waca_core_output`) in the **same BigQuery
location as your GA4 BigQuery export dataset**, and set `BQ_LOCATION` in `.env` to
match. The template defaults to `asia-northeast1` (Tokyo); change it to `US`,
`EU`, or another region if that is where your GA4 export lives.

Date handling defaults to the `Asia/Tokyo` (JST) time zone. The optional
daily-batch wrapper reads `BATCH_TIMEZONE` from `.env` (default `Asia/Tokyo`) when
it computes the date window to process, but the stored procedure body itself
currently buckets event dates in `Asia/Tokyo` and does not read `BATCH_TIMEZONE`.
If you operate outside Japan, note that day boundaries follow JST unless you edit
the SQL.

## Requirements

- A Google Cloud project.
- BigQuery enabled in that project.
- GA4 BigQuery export tables, usually named `analytics_<property_id>.events_*`,
  or the anonymous sample dataset created by this repository.
- Google Cloud SDK with the `bq` command if you run the shell scripts.
- BigQuery permission to create datasets, tables, and stored procedures.

## Legal Notice

The software code in this repository is licensed under Apache License 2.0. See
[LICENSE](LICENSE).

`WACA core` is a project name of WACA. Japan trademark application no.
商願2026-57851 was filed in 2026, and registration is pending. Other
jurisdictions may not yet be filed. Do not use WACA names in a way that implies
official certification, endorsement, or compatibility approval unless WACA has
granted permission.

This repository may include technology that is the subject of pending patent
applications or future patent applications. The Apache License 2.0 patent grant
applies as stated in the license. The trademark and patent notices above are
informational and do not replace the license text.

The software is provided "as is", without warranties or guarantees. You are
responsible for your own Google Cloud project, GA4 data, permissions, costs,
access settings, and compliance obligations.

## 日本語

WACA core は、GA4 の BigQuery export データを分析しやすい形に変換する
オープンソースの BigQuery 変換パッケージです。GA4 の管理画面レポートを見る
ものではなく、利用者自身の Google Cloud project にある raw GA4 BigQuery table
を読み、`micro_user_table` や `micro_items_table` などの分析用 table を作成します。

### 含まれるファイル

この公開 repository は、Git から install して WACA core を動かすための最小構成です。

主なファイルは次の通りです。

| Path | 役割 |
|---|---|
| `src/run_waca_core_batch.sql` | GA4 export data を変換する BigQuery stored procedure。 |
| `src/cloud_functions/waca-core-daily-batch/` | 日次実行したい場合の optional Python wrapper。 |
| `samples/bigquery/create_anonymous_ga4_sample.sql` | install rehearsal 用の匿名 sample GA4 data。 |
| `scripts/create_sample_dataset.sh` | sample dataset を作成する helper script。 |
| `scripts/install_smoke_check.sh` | static check と optional BigQuery dry-run。 |
| `.env.example` | 設定 template。`.env` に copy して自分の値に置き換えます。 |
| `INSTALL.md` | install 手順書。 |
| `CONTRIBUTING.md` | 貢献方法（DCO、Apache License 2.0）。 |
| `SECURITY.md` | セキュリティ報告の窓口（非公開）。 |
| `LICENSE` | Apache License 2.0。 |

### まず試す

```bash
git clone https://github.com/wacasg/waca-core.git
cd waca-core
cp .env.example .env
bash scripts/install_smoke_check.sh
```

この最初の check は BigQuery に接続しません。ファイル構成と公開用の基本状態だけを
確認します。Google Cloud project を指定すると、sample SQL と stored procedure SQL
の dry-run も実行できます。

```bash
PROJECT_ID=your-gcp-project-id DRY_RUN=1 bash scripts/install_smoke_check.sh
```

詳しい手順は [INSTALL.md](INSTALL.md) を参照してください。

### BigQuery location とタイムゾーン

BigQuery の query は通常 1 つの location 内で実行されるため、標準的な install では
出力 dataset（例: `waca_core_output`）を **GA4 BigQuery export dataset と同じ
BigQuery location** に作成し、`.env` の `BQ_LOCATION` も合わせてください。template の
既定値は `asia-northeast1`（東京）です。GA4 export が別 region にある場合は `US` や
`EU` などに変更してください。

日付処理は既定で `Asia/Tokyo`（JST）を使用します。任意の日次 batch wrapper は処理
対象の日付範囲を計算する際に `.env` の `BATCH_TIMEZONE`（既定 `Asia/Tokyo`）を参照
しますが、stored procedure 本体は現状 event の日付を `Asia/Tokyo` で区切っており
`BATCH_TIMEZONE` は参照しません。日本国外で運用する場合、SQL を変更しない限り日付
境界は JST 基準になる点に注意してください。

### 法的表示

この repository の software code は Apache License 2.0 で提供されます。
ライセンス本文は [LICENSE](LICENSE) にあります。

`WACA core` は WACA の project name です。日本では商願2026-57851として
2026年に商標出願済みで、登録は審査中です。WACA から許可を得ていない場合、
WACA による認定、推奨、互換性保証があるように見える使い方はしないでください。

この repository には、特許出願中または将来の特許出願対象となる可能性のある
技術が含まれる場合があります。Apache License 2.0 の patent grant はライセンス本文
の通りです。ここに書いた商標・特許に関する表示は情報提供であり、ライセンス本文を
置き換えるものではありません。

この software は現状有姿で提供されます。Google Cloud project、GA4 data、権限、
費用、access settings、compliance は利用者自身の責任で管理してください。
