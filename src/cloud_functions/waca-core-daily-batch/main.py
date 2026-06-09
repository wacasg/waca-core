"""Cloud Functions Gen2 entrypoint for WACA core daily batch execution."""

from __future__ import annotations

import logging
import json
import os
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import functions_framework
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
TARGET_DATASET = os.getenv("TARGET_DATASET", "waca_core_output")
SOURCE_DATASET = os.getenv("SOURCE_DATASET", "analytics_123456789")
# PROCEDURE_NAME must match the BigQuery stored procedure name defined in
# src/run_waca_core_batch.sql.
PROCEDURE_NAME = os.getenv("PROCEDURE_NAME", "run_waca_core_batch")
# CLIENT_ID is written to BigQuery audit tables to identify the installation.
# Set this env var to your own identifier. The default is ``default``.
CLIENT_ID = os.getenv("CLIENT_ID", "default")
BATCH_TIMEZONE = os.getenv("BATCH_TIMEZONE", "Asia/Tokyo")
INITIAL_START_DATE = os.getenv("INITIAL_START_DATE", "20260101")
INITIAL_END_DATE = os.getenv("INITIAL_END_DATE", "auto").strip()
RESET_REBUILD_START_DATE = os.getenv("RESET_REBUILD_START_DATE", "20260101")
ENABLE_SCHEMA_CHANGE_RESET = (
    os.getenv("ENABLE_SCHEMA_CHANGE_RESET", "false").strip().lower() == "true"
)
BQ_JOB_TIMEOUT_SECONDS = _env_int("BQ_JOB_TIMEOUT_SECONDS", 1700)
_LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=_LOG_LEVEL)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(_LOG_LEVEL)
SNAPSHOT_TABLE = os.getenv("SNAPSHOT_TABLE", "log_daily_batch_schema_snapshot")


def _parse_payload(request: Any) -> Dict[str, Any]:
    if request.is_json:
        return request.get_json(silent=True) or {}
    if request.data:
        try:
            return json.loads(request.data.decode("utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return None


def _validate_yyyymmdd(value: str) -> bool:
    if len(value) != 8 or not value.isdigit():
        return False
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return False
    return True


def _yesterday_yyyymmdd(tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return (datetime.now(tz).date() - timedelta(days=1)).strftime("%Y%m%d")


def _resolve_initial_end_date(yesterday: str) -> str:
    if not INITIAL_END_DATE or INITIAL_END_DATE.lower() == "auto":
        return yesterday
    if not _validate_yyyymmdd(INITIAL_END_DATE):
        LOGGER.warning(
            "INITIAL_END_DATE is invalid, fallback to yesterday: %s", INITIAL_END_DATE
        )
        return yesterday
    if INITIAL_END_DATE > yesterday:
        LOGGER.info(
            "INITIAL_END_DATE exceeds yesterday, cap to yesterday: %s -> %s",
            INITIAL_END_DATE,
            yesterday,
        )
        return yesterday
    return INITIAL_END_DATE


def _table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{TARGET_DATASET}.{table_name}"


def _dataset_info_schema() -> str:
    return f"{PROJECT_ID}.{TARGET_DATASET}.INFORMATION_SCHEMA.COLUMNS"


def _table_exists(client: bigquery.Client, table_name: str) -> bool:
    try:
        client.get_table(_table_id(table_name))
        return True
    except NotFound:
        return False


def _query_single_int(
    client: bigquery.Client,
    query: str,
    query_params: Optional[List[bigquery.ScalarQueryParameter]] = None,
) -> Optional[int]:
    job_config = bigquery.QueryJobConfig(query_parameters=query_params or [])
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None
    return int(rows[0][0]) if rows[0][0] is not None else None


def _ensure_snapshot_table(client: bigquery.Client) -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS `{_table_id(SNAPSHOT_TABLE)}` (
      snapshot_at TIMESTAMP,
      trigger_type STRING,
      start_date STRING,
      end_date STRING,
      is_force_reset BOOL,
      is_incremental BOOL,
      pre_change_fields STRING,
      post_change_fields STRING,
      rerun_force_reset BOOL,
      mst_event_params_cnt INT64,
      mst_item_params_cnt INT64,
      micro_user_columns_cnt INT64,
      micro_items_columns_cnt INT64,
      client_id STRING,
      created_by STRING
    )
    """
    client.query(query).result()
    client.query(
        f"ALTER TABLE `{_table_id(SNAPSHOT_TABLE)}` ADD COLUMN IF NOT EXISTS client_id STRING"
    ).result()


def _load_latest_snapshot(client: bigquery.Client) -> Optional[Dict[str, Any]]:
    if not _table_exists(client, SNAPSHOT_TABLE):
        return None
    query = f"""
    SELECT
      mst_event_params_cnt,
      mst_item_params_cnt,
      micro_user_columns_cnt,
      micro_items_columns_cnt
    FROM `{_table_id(SNAPSHOT_TABLE)}`
    ORDER BY snapshot_at DESC
    LIMIT 1
    """
    rows = list(client.query(query).result())
    if not rows:
        return None
    row = rows[0]
    return {
        "mst_event_params_cnt": row["mst_event_params_cnt"],
        "mst_item_params_cnt": row["mst_item_params_cnt"],
        "micro_user_columns_cnt": row["micro_user_columns_cnt"],
        "micro_items_columns_cnt": row["micro_items_columns_cnt"],
    }


def _load_current_metrics(client: bigquery.Client) -> Dict[str, Optional[int]]:
    metrics: Dict[str, Optional[int]] = {
        "mst_event_params_cnt": None,
        "mst_item_params_cnt": None,
        "micro_user_columns_cnt": None,
        "micro_items_columns_cnt": None,
    }

    if _table_exists(client, "mst_event_params"):
        metrics["mst_event_params_cnt"] = _query_single_int(
            client,
            f"SELECT COUNT(*) FROM `{_table_id('mst_event_params')}`",
        )

    if _table_exists(client, "mst_item_params"):
        metrics["mst_item_params_cnt"] = _query_single_int(
            client,
            f"SELECT COUNT(*) FROM `{_table_id('mst_item_params')}`",
        )

    for table_name, key in [
        ("micro_user_table", "micro_user_columns_cnt"),
        ("micro_items_table", "micro_items_columns_cnt"),
    ]:
        if _table_exists(client, table_name):
            metrics[key] = _query_single_int(
                client,
                f"SELECT COUNT(*) FROM `{_dataset_info_schema()}` WHERE table_name = @table_name",
                [bigquery.ScalarQueryParameter("table_name", "STRING", table_name)],
            )

    return metrics


def _detect_metric_changes(
    previous: Optional[Dict[str, Any]], current: Dict[str, Any]
) -> List[str]:
    if previous is None:
        return []

    changed: List[str] = []
    for key in ["mst_event_params_cnt", "mst_item_params_cnt"]:
        if previous.get(key) != current.get(key):
            changed.append(key)

    for key in ["micro_user_columns_cnt", "micro_items_columns_cnt"]:
        prev_val = previous.get(key)
        curr_val = current.get(key)
        if prev_val is not None and curr_val is not None:
            if curr_val < prev_val:
                changed.append(key)
        elif prev_val != curr_val:
            changed.append(key)

    return changed


def _has_batch_history(client: bigquery.Client) -> bool:
    if not _table_exists(client, "log_batch_execution"):
        return False
    count = _query_single_int(
        client,
        f"SELECT COUNT(*) FROM `{_table_id('log_batch_execution')}`",
    )
    return bool(count and count > 0)


def _find_skipped_dates_with_data(
    client: bigquery.Client, lookback_days: int = 7
) -> List[str]:
    """直近N日のうちSKIPPEDで記録されたが、今はGA4データが存在する日付を返す（YYYYMMDD文字列リスト）。"""
    if not _table_exists(client, "log_batch_execution"):
        return []
    query = f"""
    SELECT end_date
    FROM `{_table_id('log_batch_execution')}`
    WHERE phase_0_status LIKE 'SKIPPED%'
      AND end_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL @lookback_days DAY))
    ORDER BY end_date
    """
    rows = list(
        client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days)
                ]
            ),
        ).result()
    )
    skipped_dates = [row[0] for row in rows]
    if not skipped_dates:
        return []

    # GA4ソーステーブルに実データが存在するものだけを抽出
    recovered = []
    for d in skipped_dates:
        table_ref = f"{PROJECT_ID}.{SOURCE_DATASET}.events_{d}"
        try:
            t = client.get_table(table_ref)
            if t.num_rows and t.num_rows > 0:
                recovered.append(d)
        except NotFound:
            pass
    return recovered


def _run_batch(
    client: bigquery.Client,
    start_date: str,
    end_date: str,
    is_force_reset: bool,
    is_incremental: bool,
) -> None:
    call_sql = f"""
    CALL `{_table_id(PROCEDURE_NAME)}`(
      @start_date_str,
      @end_date_str,
      @project_id,
      @target_ds,
      @source_ds,
      @is_force_reset,
      @is_incremental,
      @client_id
    )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date_str", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date_str", "STRING", end_date),
            bigquery.ScalarQueryParameter("project_id", "STRING", PROJECT_ID),
            bigquery.ScalarQueryParameter("target_ds", "STRING", TARGET_DATASET),
            bigquery.ScalarQueryParameter("source_ds", "STRING", SOURCE_DATASET),
            bigquery.ScalarQueryParameter("is_force_reset", "BOOL", is_force_reset),
            bigquery.ScalarQueryParameter("is_incremental", "BOOL", is_incremental),
            bigquery.ScalarQueryParameter("client_id", "STRING", CLIENT_ID),
        ]
    )
    job_config.job_timeout_ms = max(BQ_JOB_TIMEOUT_SECONDS, 1) * 1000
    job = client.query(call_sql, job_config=job_config)
    LOGGER.info(
        "BigQuery procedure job started: job_id=%s, start_date=%s, end_date=%s, force_reset=%s, incremental=%s, timeout_sec=%s",
        job.job_id,
        start_date,
        end_date,
        is_force_reset,
        is_incremental,
        BQ_JOB_TIMEOUT_SECONDS,
    )
    try:
        job.result(timeout=max(BQ_JOB_TIMEOUT_SECONDS, 1))
    except FuturesTimeoutError as exc:
        LOGGER.error("BigQuery procedure timed out: job_id=%s", job.job_id)
        try:
            job.cancel()
        except Exception:
            LOGGER.exception("Failed to cancel timed out BigQuery job: job_id=%s", job.job_id)
        raise TimeoutError(
            f"BigQuery procedure exceeded timeout ({BQ_JOB_TIMEOUT_SECONDS}s). job_id={job.job_id}"
        ) from exc


def _insert_snapshot(
    client: bigquery.Client,
    trigger_type: str,
    start_date: str,
    end_date: str,
    is_force_reset: bool,
    is_incremental: bool,
    pre_change_fields: List[str],
    post_change_fields: List[str],
    rerun_force_reset: bool,
    metrics: Dict[str, Optional[int]],
) -> None:
    query = f"""
    INSERT INTO `{_table_id(SNAPSHOT_TABLE)}` (
      snapshot_at,
      trigger_type,
      start_date,
      end_date,
      is_force_reset,
      is_incremental,
      pre_change_fields,
      post_change_fields,
      rerun_force_reset,
      mst_event_params_cnt,
      mst_item_params_cnt,
      micro_user_columns_cnt,
      micro_items_columns_cnt,
      client_id,
      created_by
    ) VALUES (
      CURRENT_TIMESTAMP(),
      @trigger_type,
      @start_date,
      @end_date,
      @is_force_reset,
      @is_incremental,
      @pre_change_fields,
      @post_change_fields,
      @rerun_force_reset,
      @mst_event_params_cnt,
      @mst_item_params_cnt,
      @micro_user_columns_cnt,
      @micro_items_columns_cnt,
      @client_id,
      'cloud_function_waca_core_daily'
    )
    """
    params = [
        bigquery.ScalarQueryParameter("trigger_type", "STRING", trigger_type),
        bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
        bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        bigquery.ScalarQueryParameter("is_force_reset", "BOOL", is_force_reset),
        bigquery.ScalarQueryParameter("is_incremental", "BOOL", is_incremental),
        bigquery.ScalarQueryParameter(
            "pre_change_fields", "STRING", ",".join(pre_change_fields)
        ),
        bigquery.ScalarQueryParameter(
            "post_change_fields", "STRING", ",".join(post_change_fields)
        ),
        bigquery.ScalarQueryParameter(
            "rerun_force_reset", "BOOL", rerun_force_reset
        ),
        bigquery.ScalarQueryParameter(
            "mst_event_params_cnt", "INT64", metrics.get("mst_event_params_cnt")
        ),
        bigquery.ScalarQueryParameter(
            "mst_item_params_cnt", "INT64", metrics.get("mst_item_params_cnt")
        ),
        bigquery.ScalarQueryParameter(
            "micro_user_columns_cnt", "INT64", metrics.get("micro_user_columns_cnt")
        ),
        bigquery.ScalarQueryParameter(
            "micro_items_columns_cnt", "INT64", metrics.get("micro_items_columns_cnt")
        ),
        bigquery.ScalarQueryParameter("client_id", "STRING", CLIENT_ID),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def _json_response(payload: Dict[str, Any], status: int) -> Any:
    return (
        json.dumps(payload, ensure_ascii=False),
        status,
        {"Content-Type": "application/json; charset=utf-8"},
    )


@functions_framework.http
def run_waca_core_daily_batch(request: Any) -> Any:
    payload = _parse_payload(request)

    start_date_raw = payload.get("start_date")
    end_date_raw = payload.get("end_date")
    start_date = str(start_date_raw) if start_date_raw is not None else None
    end_date = str(end_date_raw) if end_date_raw is not None else None
    if (start_date and not end_date) or (end_date and not start_date):
        return _json_response(
            {
                "status": "error",
                "message": "start_date と end_date は両方指定してください。",
            },
            400,
        )

    if start_date and end_date:
        if not (_validate_yyyymmdd(start_date) and _validate_yyyymmdd(end_date)):
            return _json_response(
                {
                    "status": "error",
                    "message": "start_date/end_date は YYYYMMDD 形式で指定してください。",
                },
                400,
            )

    force_reset_override = _parse_bool(payload.get("force_reset"))
    incremental_override = _parse_bool(payload.get("is_incremental"))
    dry_run = _parse_bool(payload.get("dry_run")) is True
    LOGGER.info(
        "Request received: dry_run=%s, start_date=%s, end_date=%s, force_reset_override=%s, is_incremental_override=%s",
        dry_run,
        start_date,
        end_date,
        force_reset_override,
        incremental_override,
    )

    client = bigquery.Client(project=PROJECT_ID)
    has_history = _has_batch_history(client)
    yesterday = _yesterday_yyyymmdd(BATCH_TIMEZONE)

    trigger_type = "daily"
    if start_date and end_date:
        trigger_type = "manual"
    elif not has_history:
        trigger_type = "initial_backfill"
        start_date = INITIAL_START_DATE
        end_date = _resolve_initial_end_date(yesterday)
    else:
        start_date = yesterday
        end_date = yesterday

    skipped_recovery_candidates: List[str] = []
    # SKIPPED日の自動補完候補: 同一HTTPリクエスト内では実行しない。
    # Recovery batches can be long-running; executing them inline risks HTTP 504
    # and concurrent destructive SQL. The SQL procedure lock is the final guard,
    # but the caller should still avoid fan-in work inside a scheduler request.
    if trigger_type == "daily":
        skipped_recovery_candidates = _find_skipped_dates_with_data(client, lookback_days=7)
        if skipped_recovery_candidates:
            LOGGER.info(
                "Deferred %d SKIPPED recovery candidate(s) with GA4 data: %s",
                len(skipped_recovery_candidates),
                skipped_recovery_candidates,
            )

    previous_metrics = _load_latest_snapshot(client)
    pre_run_metrics = _load_current_metrics(client)
    pre_change_fields = _detect_metric_changes(previous_metrics, pre_run_metrics)
    is_incremental = True if incremental_override is None else incremental_override

    force_reset = False
    if force_reset_override is not None:
        force_reset = force_reset_override
    elif trigger_type == "initial_backfill":
        force_reset = True
    elif ENABLE_SCHEMA_CHANGE_RESET and pre_change_fields:
        force_reset = True
        if trigger_type == "daily":
            start_date = RESET_REBUILD_START_DATE
            end_date = yesterday
            trigger_type = "daily_schema_reset"
    LOGGER.info(
        "Execution plan: trigger_type=%s, start_date=%s, end_date=%s, force_reset=%s, is_incremental=%s, pre_change_fields=%s",
        trigger_type,
        start_date,
        end_date,
        force_reset,
        is_incremental,
        pre_change_fields,
    )

    response_payload: Dict[str, Any] = {
        "status": "planned" if dry_run else "running",
        "trigger_type": trigger_type,
        "project_id": PROJECT_ID,
        "target_dataset": TARGET_DATASET,
        "source_dataset": SOURCE_DATASET,
        "start_date": start_date,
        "end_date": end_date,
        "is_force_reset": force_reset,
        "is_incremental": is_incremental,
        "pre_change_fields": pre_change_fields,
        "pre_run_metrics": pre_run_metrics,
        "skipped_recovery_candidates": skipped_recovery_candidates,
        "recovery_execution": "deferred",
    }

    if dry_run:
        return _json_response(response_payload, 200)

    try:
        _run_batch(client, start_date, end_date, force_reset, is_incremental)

        post_run_metrics = _load_current_metrics(client)
        post_change_fields = _detect_metric_changes(pre_run_metrics, post_run_metrics)

        rerun_force_reset = False
        if ENABLE_SCHEMA_CHANGE_RESET and post_change_fields and not force_reset:
            rerun_force_reset = True
            rerun_start = (
                RESET_REBUILD_START_DATE if trigger_type.startswith("daily") else start_date
            )
            rerun_end = yesterday if trigger_type.startswith("daily") else end_date
            LOGGER.warning(
                "Post-run schema reset rerun triggered: fields=%s, rerun_start=%s, rerun_end=%s",
                post_change_fields,
                rerun_start,
                rerun_end,
            )
            _run_batch(client, rerun_start, rerun_end, True, is_incremental)
            start_date = rerun_start
            end_date = rerun_end
            trigger_type = "post_run_schema_reset"
            force_reset = True
            post_run_metrics = _load_current_metrics(client)

        # is_force_reset=TRUE の実行でテーブルがDROPされる可能性があるため、記録直前に再作成する
        _ensure_snapshot_table(client)

        _insert_snapshot(
            client=client,
            trigger_type=trigger_type,
            start_date=start_date,
            end_date=end_date,
            is_force_reset=force_reset,
            is_incremental=is_incremental,
            pre_change_fields=pre_change_fields,
            post_change_fields=post_change_fields,
            rerun_force_reset=rerun_force_reset,
            metrics=post_run_metrics,
        )
        LOGGER.info(
            "Execution success: trigger_type=%s, start_date=%s, end_date=%s, force_reset=%s, rerun_force_reset=%s, post_change_fields=%s",
            trigger_type,
            start_date,
            end_date,
            force_reset,
            rerun_force_reset,
            post_change_fields,
        )
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        LOGGER.exception("Execution failed: %s", error_message)
        failure_metrics = _load_current_metrics(client)

        try:
            _ensure_snapshot_table(client)
            _insert_snapshot(
                client=client,
                trigger_type=f"{trigger_type}_error",
                start_date=start_date,
                end_date=end_date,
                is_force_reset=force_reset,
                is_incremental=is_incremental,
                pre_change_fields=pre_change_fields,
                post_change_fields=[error_message[:500]],
                rerun_force_reset=False,
                metrics=failure_metrics,
            )
        except Exception:
            LOGGER.exception("Failed to record error snapshot.")

        response_payload.update(
            {
                "status": "error",
                "error_message": error_message,
                "post_run_metrics": failure_metrics,
            }
        )
        return _json_response(response_payload, 500)

    response_payload.update(
        {
            "status": "success",
            "trigger_type": trigger_type,
            "start_date": start_date,
            "end_date": end_date,
            "is_force_reset": force_reset,
            "rerun_force_reset": rerun_force_reset,
            "post_change_fields": post_change_fields,
            "post_run_metrics": post_run_metrics,
        }
    )

    return _json_response(response_payload, 200)
