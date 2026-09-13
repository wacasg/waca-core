-- WACA core BigQuery batch procedure.
--
-- Transforms GA4 BigQuery export tables into analysis-ready WACA core tables.
-- This public release is intended to run inside the installer's own Google Cloud project.

CREATE OR REPLACE PROCEDURE `your-gcp-project-id.your-dataset-id.run_waca_core_batch`(
  IN start_date_str STRING,
  IN end_date_str STRING,
  IN project_id STRING,
  IN target_ds STRING,
  IN source_ds STRING,
  IN is_force_reset BOOL,
  IN is_incremental BOOL,
  IN client_id STRING           -- クライアント識別子（例: 'your-client-id'）
)
BEGIN
  -- ================================================================================
  -- 1. 共通変数宣言
  -- ================================================================================
  -- 全Phase共通
  DECLARE q STRING DEFAULT "'";
  DECLARE full_target_path STRING DEFAULT CONCAT(project_id, '.', target_ds);
  DECLARE full_source_path STRING DEFAULT CONCAT(project_id, '.', source_ds);
  DECLARE sql_text STRING;
  DECLARE batch_timezone STRING DEFAULT 'Asia/Tokyo';
  DECLARE batch_id STRING DEFAULT GENERATE_UUID();
  DECLARE batch_lock_name STRING DEFAULT 'run_waca_core_batch';
  DECLARE batch_lock_acquired BOOL DEFAULT FALSE;

  -- Phase制御
  DECLARE phase_0_status STRING DEFAULT 'PENDING';
  DECLARE phase_1_status STRING DEFAULT 'PENDING';
  DECLARE phase_2_status STRING DEFAULT 'PENDING';
  DECLARE phase_3a_status STRING DEFAULT 'PENDING';
  DECLARE phase_3b_status STRING DEFAULT 'PENDING';
  DECLARE batch_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE ga4_table_exists BOOL DEFAULT FALSE;
  DECLARE ga4_row_count INT64 DEFAULT 0;
  DECLARE ga4_table_count INT64 DEFAULT 0;

  -- Phase 0 固有変数
  DECLARE missing_columns_sql STRING;
  -- 公開向けパラメータ除外条件の差し込み口。OSS 既定では追加除外なし（空文字列）。
  DECLARE ignored_subquery STRING DEFAULT '';

  -- Phase 1 固有変数
  DECLARE p1_start_date DATE;
  DECLARE p1_end_date DATE;
  DECLARE p1_lookback_start_date DATE;
  DECLARE p1_lookback_start_suffix STRING;
  DECLARE p1_end_date_suffix STRING;
  DECLARE p1_dynamic_columns_sql STRING;
  DECLARE p1_create_table_sql STRING;
  DECLARE p1_result_count_1 INT64 DEFAULT 0;
  DECLARE p1_result_count_2 INT64 DEFAULT 0;
  DECLARE p1_result_count_3 INT64 DEFAULT 0;
  DECLARE p1_has_pseudonymous_users BOOL DEFAULT FALSE;

  -- Phase 2 固有変数
  DECLARE p2_columns_sql STRING;
  DECLARE p2_create_table_sql STRING;
  DECLARE p2_table_suffix_start STRING;
  DECLARE p2_table_suffix_end STRING;
  DECLARE p2_start_date_formatted STRING;
  DECLARE p2_end_date_formatted STRING;
  DECLARE p2_event_count INT64;
  DECLARE p2_processed_count INT64 DEFAULT 0;
  DECLARE p2_current_event_name STRING;
  DECLARE p2_sql_for_columns STRING;
  DECLARE p2_standard_columns STRING;
  DECLARE p2_table_exists BOOL DEFAULT FALSE;

  -- Phase 3 固有変数
  DECLARE p3_dynamic_schema_sql STRING;
  DECLARE p3_dynamic_col_names_sql STRING;
  DECLARE p3_insert_col_list STRING;
  DECLARE p3_insert_select_sql STRING;
  DECLARE p3_current_event STRING;
  DECLARE p3_result_count INT64 DEFAULT 0;
  DECLARE p3_processed_events INT64 DEFAULT 0;
  DECLARE p3_slot_update_set STRING;
  DECLARE p3_is_incremental BOOL;
  DECLARE p3_date_filter STRING;
  DECLARE p3_table_exists BOOL;
  DECLARE p3_schema_ok BOOL DEFAULT FALSE;
  DECLARE p3_scroll_exists BOOL;
  DECLARE p3_start_date DATE;
  DECLARE p3_end_date DATE;
  DECLARE p3_failed_events ARRAY<STRING> DEFAULT [];
  DECLARE p3_success_count INT64 DEFAULT 0;
  DECLARE p3_failed_errors ARRAY<STRING> DEFAULT [];
  DECLARE p3_first_error_msg STRING DEFAULT NULL;
  DECLARE p3b_table_suffix_start STRING;
  DECLARE p3b_table_suffix_end STRING;
  DECLARE p3b_select_sql STRING;
  DECLARE p3b_item_params_sql STRING;
  DECLARE p3b_create_sql STRING;
  DECLARE p3b_event_count INT64 DEFAULT 0;
  DECLARE p3b_column_list STRING;
  DECLARE p3b_item_col_names STRING;
  DECLARE p3b_failed_events ARRAY<STRING> DEFAULT [];
  DECLARE p3b_success_count INT64 DEFAULT 0;
  DECLARE p3b_failed_errors ARRAY<STRING> DEFAULT [];
  DECLARE p3b_first_error_msg STRING DEFAULT NULL;
  DECLARE p1_audience_warning STRING DEFAULT '';
  -- Dynamic parameter exclusion rules are reused by schema reconciliation and
  -- micro table generation. Add new excluded prefixes/keys here.
  DECLARE p_exclude_prefix_1 STRING DEFAULT 'batch_';
  DECLARE p_exclude_prefix_2 STRING DEFAULT 'clarity_';
  DECLARE p_exclude_key STRING DEFAULT 'gclid';


  -- ================================================================================
  -- 2. バッチ実行ログテーブル作成
  -- ================================================================================
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `%s.log_batch_execution` (
      batch_id STRING,
      executed_at TIMESTAMP,
      start_date STRING,
      end_date STRING,
      phase_0_status STRING,
      phase_1_status STRING,
      phase_2_status STRING,
      phase_3a_status STRING,
      phase_3b_status STRING,
      is_force_reset BOOL,
      is_incremental BOOL,
      total_rows INT64,
      execution_time_seconds FLOAT64,
      processor_version STRING,
      client_id STRING
    )
  """, full_target_path);

  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `%s.log_batch_lock` (
      lock_name STRING,
      batch_id STRING,
      acquired_at TIMESTAMP,
      expires_at TIMESTAMP,
      start_date STRING,
      end_date STRING,
      client_id STRING
    )
  """, full_target_path);

  -- BigQuery procedure can also be invoked manually, so the lock lives inside SQL,
  -- not only in the Cloud Function caller.
  BEGIN TRANSACTION;
    EXECUTE IMMEDIATE FORMAT("""
      DELETE FROM `%s.log_batch_lock`
      WHERE lock_name = @p_lock_name
        AND expires_at < CURRENT_TIMESTAMP()
    """, full_target_path)
    USING batch_lock_name AS p_lock_name;
    -- BigQuery does not support a bare INSERT ... SELECT @const WHERE NOT EXISTS
    -- without a FROM clause, so the lock row is inserted through a source struct.
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.log_batch_lock`
      (lock_name, batch_id, acquired_at, expires_at, start_date, end_date, client_id)
      SELECT
        s.lock_name,
        s.batch_id,
        s.acquired_at,
        s.expires_at,
        s.start_date,
        s.end_date,
        s.client_id
      FROM UNNEST([STRUCT(
        @p_lock_name AS lock_name,
        @p_batch_id AS batch_id,
        CURRENT_TIMESTAMP() AS acquired_at,
        TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 180 MINUTE) AS expires_at,
        @p_start_date AS start_date,
        @p_end_date AS end_date,
        @p_client_id AS client_id
      )]) AS s
      WHERE NOT EXISTS (
        SELECT 1
        FROM `%s.log_batch_lock`
        WHERE lock_name = @p_lock_name
          AND expires_at >= CURRENT_TIMESTAMP()
      )
    """, full_target_path, full_target_path)
    USING
      batch_lock_name AS p_lock_name,
      batch_id AS p_batch_id,
      start_date_str AS p_start_date,
      end_date_str AS p_end_date,
      client_id AS p_client_id;

    EXECUTE IMMEDIATE FORMAT("""
      SELECT COUNT(*) > 0
      FROM `%s.log_batch_lock`
      WHERE lock_name = @p_lock_name
        AND batch_id = @p_batch_id
    """, full_target_path)
    INTO batch_lock_acquired
    USING batch_lock_name AS p_lock_name, batch_id AS p_batch_id;
  COMMIT TRANSACTION;

  IF NOT batch_lock_acquired THEN
    RAISE USING MESSAGE = 'Another run_waca_core_batch execution is already active for this dataset. Retry after the current batch finishes or the lock expires.';
  END IF;

  -- ================================================================================
  BEGIN
    SET sql_text = FORMAT("""
      SELECT COUNT(*) FROM `%s.__TABLES__`
      WHERE table_id BETWEEN CONCAT('events_', '%s') AND CONCAT('events_', '%s')
    """, full_source_path, start_date_str, end_date_str);
    EXECUTE IMMEDIATE sql_text INTO ga4_table_count;
    SET ga4_table_exists = (ga4_table_count > 0);

    IF ga4_table_exists THEN
      SET sql_text = FORMAT("""
        SELECT COUNT(*) FROM `%s.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '%s' AND '%s'
      """, full_source_path, start_date_str, end_date_str);
      EXECUTE IMMEDIATE sql_text INTO ga4_row_count;
    END IF;
  EXCEPTION WHEN ERROR THEN
    SET ga4_table_exists = FALSE;
    SET ga4_row_count = 0;
    SET ga4_table_count = 0;
  END;

  -- GA4データが存在しない場合は全Phaseスキップ
  IF NOT ga4_table_exists OR ga4_row_count = 0 THEN
    SET phase_0_status = CONCAT('SKIPPED: GA4 events_', start_date_str, ' not found or empty (row_count=', CAST(ga4_row_count AS STRING), ')');
    SET phase_1_status = 'SKIPPED: No GA4 data';
    SET phase_2_status = 'SKIPPED: No GA4 data';
    SET phase_3a_status = 'SKIPPED: No GA4 data';
    SET phase_3b_status = 'SKIPPED: No GA4 data';

    -- ログ記録して終了
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.log_batch_execution`
      (batch_id, executed_at, start_date, end_date,
       phase_0_status, phase_1_status, phase_2_status, phase_3a_status, phase_3b_status,
       is_force_reset, is_incremental, total_rows, execution_time_seconds,
       processor_version, client_id)
      VALUES (
        '%s', CURRENT_TIMESTAMP(),
        '%s', '%s',
        '%s', '%s', '%s', '%s', '%s',
        %s, %s, 0,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP '%s', SECOND),
        'public-v0.1.0', '%s'
      )
    """,
      full_target_path,
      batch_id,
      start_date_str, end_date_str,
      phase_0_status, phase_1_status, phase_2_status, phase_3a_status, phase_3b_status,
      CAST(is_force_reset AS STRING), CAST(is_incremental AS STRING),
      CAST(batch_start_time AS STRING),
      client_id
    );

    EXECUTE IMMEDIATE FORMAT("""
      DELETE FROM `%s.log_batch_lock`
      WHERE lock_name = @p_lock_name AND batch_id = @p_batch_id
    """, full_target_path)
    USING batch_lock_name AS p_lock_name, batch_id AS p_batch_id;

    SELECT 'WACA core batch public-v0.1.0' AS processor,
      start_date_str AS start_date, end_date_str AS end_date,
      phase_0_status, phase_1_status, phase_2_status, phase_3a_status, phase_3b_status,
      'ABORTED: No GA4 data' AS update_mode,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), batch_start_time, SECOND) AS total_seconds;
    RETURN;
  END IF;

  -- ================================================================================
  IF start_date_str = end_date_str AND NOT is_incremental THEN
    SET phase_3a_status = CONCAT('WARNING: Single-day run (', start_date_str,
      ') with is_incremental=FALSE will FULL_REBUILD micro_user_table. ',
      'Historical data will be lost. Use is_incremental=TRUE for daily batch.');
  END IF;

  -- ================================================================================
  -- Phase 0: dataset setup, master tables, and schema reconciliation.
  -- ==================================================================================
  -- Uses FORMAT-based dynamic SQL for dataset-qualified DDL/DML.
  BEGIN
    -- Phase 0-1: データセット・履歴テーブルの初期化
    EXECUTE IMMEDIATE FORMAT("""
      CREATE SCHEMA IF NOT EXISTS `%s`
    """, full_target_path);

    EXECUTE IMMEDIATE FORMAT("""
      CREATE TABLE IF NOT EXISTS `%s.mst_schema_history` (
        executed_at TIMESTAMP, column_name STRING, data_type STRING, reason STRING
      )
    """, full_target_path);

    EXECUTE IMMEDIATE FORMAT("""
      CREATE TABLE IF NOT EXISTS `%s.audit_schema_log` (
        column_name STRING, status STRING, reason STRING,
        original_data_types STRING, events_involved STRING
      )
    """, full_target_path);

    EXECUTE IMMEDIATE FORMAT("""
      CREATE TABLE IF NOT EXISTS `%s.log_integrated_columns` (
        source_table STRING, integrated_column STRING, reason STRING, processed_at TIMESTAMP
      )
    """, full_target_path);
        EXECUTE IMMEDIATE FORMAT("""
      CREATE TABLE IF NOT EXISTS `%s.micro_user_table` (
        event_date DATE, event_timestamp DATETIME, event_timestamp_micros INT64,
        event_name STRING,
        user_pseudo_id STRING, user_id STRING, is_identified_user BOOL,
        ga_session_id INT64, page_view_id STRING, is_key_event INT64,
        event_id STRING,
        session_event_no INT64, user_event_no INT64,
        hostname STRING, hostname_norm STRING,
        prev_hostname_norm STRING, next_hostname_norm STRING,
        is_cross_domain_hop BOOL,
        prev_event_name STRING, seconds_from_prev_event INT64,
        batch_id STRING
      )
      PARTITION BY event_date
      CLUSTER BY user_pseudo_id, user_id
    """, full_target_path);

    -- Phase 0-2: GA4標準パラメータ型定義辞書
    CREATE OR REPLACE TEMP TABLE standard_config AS
      SELECT 'percent_scrolled' as key, 'FLOAT64' as type UNION ALL
      SELECT 'link_url', 'STRING' UNION ALL SELECT 'link_id', 'STRING' UNION ALL
      SELECT 'link_classes', 'STRING' UNION ALL SELECT 'link_domain', 'STRING' UNION ALL
      SELECT 'outbound', 'STRING' UNION ALL SELECT 'search_term', 'STRING' UNION ALL
      SELECT 'file_extension', 'STRING' UNION ALL SELECT 'file_name', 'STRING' UNION ALL
      SELECT 'video_provider', 'STRING' UNION ALL SELECT 'video_title', 'STRING' UNION ALL
      SELECT 'video_url', 'STRING' UNION ALL
      SELECT 'video_current_time', 'FLOAT64' UNION ALL SELECT 'video_duration', 'FLOAT64' UNION ALL
      SELECT 'video_percent', 'FLOAT64' UNION ALL SELECT 'visible', 'STRING' UNION ALL
      SELECT 'value', 'FLOAT64' UNION ALL SELECT 'currency', 'STRING' UNION ALL
      SELECT 'transaction_id', 'STRING' UNION ALL SELECT 'tax', 'FLOAT64' UNION ALL
      SELECT 'shipping', 'FLOAT64' UNION ALL SELECT 'coupon', 'STRING' UNION ALL
      SELECT 'payment_type', 'STRING' UNION ALL SELECT 'shipping_tier', 'STRING' UNION ALL
      SELECT 'affiliation', 'STRING' UNION ALL SELECT 'item_id', 'STRING' UNION ALL
      SELECT 'item_name', 'STRING' UNION ALL SELECT 'item_brand', 'STRING' UNION ALL
      SELECT 'item_variant', 'STRING' UNION ALL SELECT 'item_category', 'STRING' UNION ALL
      SELECT 'item_category2', 'STRING' UNION ALL SELECT 'item_category3', 'STRING' UNION ALL
      SELECT 'item_category4', 'STRING' UNION ALL SELECT 'item_category5', 'STRING' UNION ALL
      SELECT 'price', 'FLOAT64' UNION ALL SELECT 'quantity', 'INT64' UNION ALL
      SELECT 'discount', 'FLOAT64' UNION ALL SELECT 'promotion_id', 'STRING' UNION ALL
      SELECT 'promotion_name', 'STRING' UNION ALL SELECT 'creative_name', 'STRING' UNION ALL
      SELECT 'creative_slot', 'STRING' UNION ALL SELECT 'location_id', 'STRING' UNION ALL
      SELECT 'method', 'STRING' UNION ALL SELECT 'content_type', 'STRING' UNION ALL
      SELECT 'level', 'INT64' UNION ALL SELECT 'score', 'INT64' UNION ALL
      SELECT 'virtual_currency_name', 'STRING' UNION ALL SELECT 'character', 'STRING' UNION ALL
      SELECT 'group_id', 'STRING' UNION ALL SELECT 'achievement_id', 'STRING' UNION ALL
      SELECT 'page_location', 'STRING' UNION ALL SELECT 'page_referrer', 'STRING' UNION ALL
      SELECT 'page_title', 'STRING' UNION ALL SELECT 'engagement_time_msec', 'INT64' UNION ALL
      SELECT 'ga_session_id', 'INT64' UNION ALL SELECT 'ga_session_number', 'INT64';

    -- Phase 0-3: マスタリセット
    IF is_force_reset THEN
      EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.mst_event_params`", full_target_path);
      EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.mst_duplicate_params_log`", full_target_path);
      EXECUTE IMMEDIATE FORMAT("TRUNCATE TABLE `%s.audit_schema_log`", full_target_path);
      EXECUTE IMMEDIATE FORMAT("TRUNCATE TABLE `%s.log_integrated_columns`", full_target_path);
        EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.log_audience_membership`", full_target_path);
      EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.log_user_properties`", full_target_path);
      EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.mst_user_properties`", full_target_path);
    END IF;

    -- Step 0: 生データからパラメータ候補を抽出
    -- events_* に alias `e` を付与し、将来の公開向け除外条件を差し込める形にする。
    SET sql_text = FORMAT("""
      CREATE OR REPLACE TABLE `%s.mst_event_params_raw` AS
      WITH observed AS (
        SELECT DISTINCT e.event_name AS event_name, ep.key AS param_name,
          CASE
            WHEN ep.value.int_value IS NOT NULL THEN 'INT64'
            WHEN ep.value.double_value IS NOT NULL THEN 'FLOAT64'
            ELSE 'STRING'
          END AS data_type
        FROM `%s.events_*` e, UNNEST(e.event_params) AS ep
        WHERE e._TABLE_SUFFIX BETWEEN '%s' AND '%s'
          AND ep.key NOT IN (
            'ga_session_id', 'ga_session_number', 'session_engaged', 'entrances',
            'batch_page_id', 'batch_ordering_id', 'ignore_referrer', 'debug_mode'
          )
          %s
      )
      SELECT obs.event_name, obs.param_name, COALESCE(cfg.type, obs.data_type) as data_type
      FROM observed obs LEFT JOIN standard_config cfg ON obs.param_name = cfg.key
    """, full_target_path, full_source_path, start_date_str, end_date_str, ignored_subquery);
    EXECUTE IMMEDIATE sql_text;
    SET sql_text = FORMAT("SELECT COUNT(*) FROM `%s.mst_event_params_raw`", full_target_path);
    EXECUTE IMMEDIATE sql_text INTO ga4_row_count;
    IF ga4_row_count = 0 THEN
      EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.mst_event_params_raw`", full_target_path);
      RAISE USING MESSAGE = CONCAT('GA4 events_', start_date_str, ' exists but mst_event_params_raw is empty. GA4 data may not be fully exported yet.');
    END IF;
    -- まずmst_event_paramsテーブルが存在するか確認
    EXECUTE IMMEDIATE FORMAT("""
      CREATE TABLE IF NOT EXISTS `%s.mst_event_params` (
        event_name STRING, param_name STRING, data_type STRING,
        micro_column_name STRING,
        event_category STRING,
        param_category STRING,
        created_at TIMESTAMP
      )
    """, full_target_path);

    -- 新しいデータをTEMPテーブルに準備
    SET sql_text = FORMAT("""
      CREATE OR REPLACE TEMP TABLE _new_event_params AS
      SELECT event_name, param_name,
        CASE
          WHEN COUNTIF(data_type = 'STRING') > 0 THEN 'STRING'
          WHEN COUNTIF(data_type = 'FLOAT64') > 0 THEN 'FLOAT64'
          ELSE 'INT64'
        END AS data_type,
        param_name AS micro_column_name,
        CURRENT_TIMESTAMP() AS created_at
      FROM `%s.mst_event_params_raw` GROUP BY event_name, param_name
    """, full_target_path);
    EXECUTE IMMEDIATE sql_text;    -- Once a column is STRING, keep it STRING to avoid type conflicts.
    -- MATCHED rows reconcile target and source types by priority.
    SET sql_text = FORMAT("""
      MERGE `%s.mst_event_params` AS target
      USING _new_event_params AS source
      ON target.event_name = source.event_name AND target.param_name = source.param_name
      WHEN MATCHED THEN
        UPDATE SET
          data_type = CASE
            WHEN target.data_type = 'STRING'  OR source.data_type = 'STRING'  THEN 'STRING'
            WHEN target.data_type = 'FLOAT64' OR source.data_type = 'FLOAT64' THEN 'FLOAT64'
            ELSE 'INT64'
          END,
          micro_column_name = source.micro_column_name,
          created_at = source.created_at
      WHEN NOT MATCHED THEN
        INSERT (event_name, param_name, data_type, micro_column_name, created_at)
        VALUES (source.event_name, source.param_name, source.data_type, source.micro_column_name, source.created_at)
    """, full_target_path);
    EXECUTE IMMEDIATE sql_text;
    EXECUTE IMMEDIATE FORMAT("""
      DELETE FROM `%s.mst_event_params` WHERE event_name = 'fetch_user_data'
    """, full_target_path);
        EXECUTE IMMEDIATE FORMAT("""
      UPDATE `%s.mst_event_params`
      SET event_category = CASE
        WHEN event_name IN ('first_visit','session_start','user_engagement') THEN 'auto'
        WHEN event_name IN ('page_view','scroll','click','view_search_results',
                             'file_download','video_start','video_progress','video_complete',
                             'form_start','form_submit') THEN 'enhanced'
        WHEN event_name IN ('purchase','add_to_cart','begin_checkout','add_payment_info',
                             'add_shipping_info','view_item','view_item_list','select_item',
                             'view_promotion','select_promotion','remove_from_cart',
                             'view_cart','refund','login','sign_up','search',
                             'level_start','level_end','level_up') THEN 'recommended'
        ELSE 'custom'
      END
      WHERE event_category IS NULL OR event_category = ''
    """, full_target_path);    -- traffic source系はauto扱い。clarity_*はmartech。辞書未登録=custom。
    -- ※ WHERE条件が NULL/'' のみのため、カテゴリ変更時は is_force_reset=TRUE で全量リビルドが必要。
    EXECUTE IMMEDIATE FORMAT("""
      UPDATE `%s.mst_event_params`
      SET param_category = CASE
        WHEN param_name IN ('page_location','page_title','page_referrer',
                             'engaged_session_event','engagement_time_msec',
                             'campaign','campaign_id','campaign_source','campaign_medium',
                             'campaign_term','campaign_content',
                             'source','medium','term','content',
                             'gclid','dclid','srsltid',
                             'gad_source','gad_campaignid') THEN 'auto'
        WHEN param_name IN ('percent_scrolled','search_term','unique_search_term',
                             'link_url','link_domain','link_classes','link_id','link_text',
                             'outbound','file_extension','file_name',
                             'video_current_time','video_duration','video_percent',
                             'video_provider','video_title','video_url','visible',
                             'form_destination','form_id','form_name','form_submit_text') THEN 'enhanced'
        WHEN param_name IN ('transaction_id','value','tax','shipping','currency',
                             'coupon','affiliation','payment_type','shipping_tier',
                             'level','level_name','character','score',
                             'achievement_id','group_id','content_type','content_id',
                             'virtual_currency_name','success',
                             'method',
                             'item_list_id','item_list_name',
                             'creative_name','creative_slot','location_id',
                             'promotion_id','promotion_name') THEN 'recommended'
        WHEN param_name LIKE 'clarity_%%' THEN 'martech'
        ELSE 'custom'
      END
      WHERE param_category IS NULL OR param_category = ''
    """, full_target_path);
        EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TABLE `%s.mst_duplicate_params_log` AS
      SELECT r.event_name, r.param_name,
        COUNT(*) as duplicate_count,
        STRING_AGG(r.data_type) as types_found,
        COALESCE(m.data_type, 'UNKNOWN') AS resolved_type
      FROM `%s.mst_event_params_raw` r
      LEFT JOIN `%s.mst_event_params` m ON r.event_name = m.event_name AND r.param_name = m.param_name
      GROUP BY r.event_name, r.param_name, m.data_type
      HAVING COUNT(*) > 1
    """, full_target_path, full_target_path, full_target_path);
        SET sql_text = FORMAT("""
      MERGE `%s.mst_event_params` AS target
      USING (SELECT 'search' AS event_name, 'search_term' AS param_name, 'STRING' AS data_type,
                    'search_term' AS micro_column_name, CURRENT_TIMESTAMP() AS created_at) AS source
      ON target.event_name = source.event_name AND target.param_name = source.param_name
      WHEN NOT MATCHED THEN
        INSERT (event_name, param_name, data_type, micro_column_name, created_at)
        VALUES (source.event_name, source.param_name, source.data_type, source.micro_column_name, source.created_at)
    """, full_target_path);
    EXECUTE IMMEDIATE sql_text;    -- value → value__{event_name} で分離（常時）
    -- custom param + custom event → {param_name}__{event_name} で分離
    -- custom param + default event → {param_name}（サフィックスなし・統合）
    -- auto/enhanced/recommended → サフィックスなし（統合）
    EXECUTE IMMEDIATE FORMAT("""
      UPDATE `%s.mst_event_params`
      SET micro_column_name = CASE
        WHEN param_name = 'value' THEN CONCAT(param_name, '__', event_name)
        WHEN param_category = 'custom' AND event_category = 'custom' THEN CONCAT(param_name, '__', event_name)
        ELSE param_name
      END
      WHERE TRUE
    """, full_target_path);

    -- micro_column_name は公開 WACA core の標準ルールで決定する。
    -- audit_schema_log への記録
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.audit_schema_log` (column_name, status, reason, original_data_types, events_involved)
      SELECT param_name, 'TYPE_UNIFIED',
        'Multiple data types found, unified by priority rule (STRING > FLOAT64 > INT64)',
        STRING_AGG(DISTINCT data_type ORDER BY data_type),
        STRING_AGG(DISTINCT event_name ORDER BY event_name)
      FROM `%s.mst_event_params_raw` GROUP BY param_name HAVING COUNT(DISTINCT data_type) > 1
    """, full_target_path, full_target_path);

    -- Step 0.75: スキーマ自動追従
    -- DISTINCT だと複数行になりALTER TABLEで重複カラムエラーが発生する。
    -- GROUP BY + 型統一ルール（STRING > FLOAT64 > INT64）で1行に集約する。
    -- Phase 3A は clarity_*/batch_*/gclid を動的カラムに含めない（固定列 or 不使用）。
    --   Step 0.75 がこれらを ALTER TABLE ADD COLUMN すると、位置指定 INSERT の
    --   カラム数不一致で Phase 3A が全件失敗する。
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE temp_missing_columns AS
      SELECT micro_column_name,
        CASE
          WHEN COUNTIF(data_type = 'STRING') > 0 THEN 'STRING'
          WHEN COUNTIF(data_type = 'FLOAT64') > 0 THEN 'FLOAT64'
          ELSE 'INT64'
        END AS data_type
      FROM `%s.mst_event_params`
      WHERE micro_column_name NOT IN (
        SELECT column_name FROM `%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'micro_user_table'
      )
        AND param_name NOT LIKE '%s%%'
        AND param_name NOT LIKE '%s%%'
        AND param_name != '%s'
      GROUP BY micro_column_name
    """, full_target_path, full_target_path, p_exclude_prefix_1, p_exclude_prefix_2, p_exclude_key);

    SET missing_columns_sql = (
      SELECT STRING_AGG(CONCAT('ADD COLUMN IF NOT EXISTS `', micro_column_name, '` ', data_type), ', ')
      FROM temp_missing_columns
    );

    IF missing_columns_sql IS NOT NULL THEN
      EXECUTE IMMEDIATE FORMAT("ALTER TABLE `%s.micro_user_table` %s", full_target_path, missing_columns_sql);
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `%s.mst_schema_history` (executed_at, column_name, data_type, reason)
        SELECT CURRENT_TIMESTAMP(), micro_column_name, data_type, 'Step 0.75: New parameter added'
        FROM temp_missing_columns
      """, full_target_path);
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `%s.log_integrated_columns` (source_table, integrated_column, reason, processed_at)
        SELECT 'micro_user_table', micro_column_name, 'Schema evolution: new parameter detected', CURRENT_TIMESTAMP()
        FROM temp_missing_columns
      """, full_target_path);
    END IF;

    -- ==================================================================================
    -- 仕様（type-sticky + 自動広域化 + バックフィル）:
    --   mst_event_params.data_type と micro_user_table の実カラム型を突き合わせ、
    --   priority (STRING > FLOAT64 > INT64) で統一する。
    --
    -- 動作:
    --         → mst_event_params を table 側に reconcile（type-sticky）
    --         → Phase 3A の CAST 式が STRING に揃い、以降のバッチで型衝突が発生しない
    --         → 過去データを丸ごとバックフィルしつつ再構築
    --   いずれの場合も mst_schema_history にログ記録される。
    --
    -- This prevents historical type narrowing from breaking later inserts.
    -- ==================================================================================
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE _p0_schema_diff AS
      WITH mst_type AS (
        SELECT micro_column_name,
          CASE
            WHEN COUNTIF(data_type = 'STRING') > 0 THEN 'STRING'
            WHEN COUNTIF(data_type = 'FLOAT64') > 0 THEN 'FLOAT64'
            ELSE 'INT64'
          END AS mst_type
        FROM `%s.mst_event_params`
        GROUP BY micro_column_name
      ),
      tbl_type AS (
        SELECT column_name AS micro_column_name, data_type AS tbl_type
        FROM `%s.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'micro_user_table'
          AND data_type IN ('STRING','FLOAT64','INT64')
      )
      SELECT m.micro_column_name, m.mst_type, t.tbl_type,
        CASE
          WHEN m.mst_type = 'STRING'  OR t.tbl_type = 'STRING'  THEN 'STRING'
          WHEN m.mst_type = 'FLOAT64' OR t.tbl_type = 'FLOAT64' THEN 'FLOAT64'
          ELSE 'INT64'
        END AS unified_type
      FROM mst_type m
      INNER JOIN tbl_type t USING (micro_column_name)
      WHERE m.mst_type != t.tbl_type
    """, full_target_path, full_target_path);
        EXECUTE IMMEDIATE FORMAT("""
      UPDATE `%s.mst_event_params` m
      SET data_type = d.tbl_type
      FROM _p0_schema_diff d
      WHERE m.micro_column_name = d.micro_column_name
        AND d.unified_type = d.tbl_type
        AND m.data_type != d.tbl_type
    """, full_target_path);

    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.mst_schema_history` (executed_at, column_name, data_type, reason)
      SELECT CURRENT_TIMESTAMP(), micro_column_name, tbl_type,
        CONCAT('Step 0.76: Reconciled mst_event_params to table type (type-sticky, was=', mst_type, ')')
      FROM _p0_schema_diff
      WHERE unified_type = tbl_type AND mst_type != tbl_type
    """, full_target_path);    FOR widen_rec IN (
      SELECT micro_column_name FROM _p0_schema_diff
      WHERE tbl_type = 'INT64' AND unified_type = 'FLOAT64'
    )
    DO
      EXECUTE IMMEDIATE FORMAT(
        "ALTER TABLE `%s.micro_user_table` ALTER COLUMN `%s` SET DATA TYPE FLOAT64",
        full_target_path, widen_rec.micro_column_name
      );
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `%s.mst_schema_history` (executed_at, column_name, data_type, reason)
        VALUES (CURRENT_TIMESTAMP(), '%s', 'FLOAT64',
                'Step 0.76: Type widened INT64 to FLOAT64 via ALTER COLUMN')
      """, full_target_path, widen_rec.micro_column_name);
    END FOR;
    SET missing_columns_sql = (
      SELECT STRING_AGG(micro_column_name, ',' ORDER BY micro_column_name)
      FROM _p0_schema_diff
      WHERE tbl_type IN ('INT64','FLOAT64') AND unified_type = 'STRING'
    );
    IF missing_columns_sql IS NOT NULL AND missing_columns_sql != '' THEN
      -- CAST対象列は CAST(col AS STRING)、その他は元カラムをそのまま使用する SELECT リストを構築
      EXECUTE IMMEDIATE FORMAT("""
        CREATE OR REPLACE TEMP TABLE _p0_rebuild_select AS
        SELECT STRING_AGG(
          CASE WHEN column_name IN UNNEST(SPLIT('%s', ','))
               THEN CONCAT('CAST(`', column_name, '` AS STRING) AS `', column_name, '`')
               ELSE CONCAT('`', column_name, '`')
          END,
          ', ' ORDER BY ordinal_position
        ) AS sel
        FROM `%s.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'micro_user_table'
      """, missing_columns_sql, full_target_path);

      SET sql_text = (SELECT sel FROM _p0_rebuild_select);
      EXECUTE IMMEDIATE CONCAT(
        "CREATE OR REPLACE TABLE `", full_target_path, ".micro_user_table` ",
        "PARTITION BY event_date CLUSTER BY user_pseudo_id, user_id AS ",
        "SELECT ", sql_text, " FROM `", full_target_path, ".micro_user_table`"
      );

      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `%s.mst_schema_history` (executed_at, column_name, data_type, reason)
        SELECT CURRENT_TIMESTAMP(), micro_column_name, 'STRING',
          CONCAT('Step 0.76: Type widened ', tbl_type, ' to STRING via table rebuild + CAST backfill')
        FROM _p0_schema_diff
        WHERE tbl_type IN ('INT64','FLOAT64') AND unified_type = 'STRING'
      """, full_target_path);
    END IF;

    -- Cleanup
    EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.mst_event_params_raw`", full_target_path);

    -- ==================================================================================

    -- mst_item_params テーブルのリセット（is_force_reset時）
    IF is_force_reset THEN
      EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s.mst_item_params`", full_target_path);
    END IF;

    -- mst_item_params テーブル作成
    EXECUTE IMMEDIATE FORMAT("""
      CREATE TABLE IF NOT EXISTS `%s.mst_item_params` (
        param_name STRING, data_type STRING,
        micro_column_name STRING, created_at TIMESTAMP
      )
      CLUSTER BY param_name
    """, full_target_path);

    -- items内のitem_paramsを抽出し、型統一（STRING > FLOAT64 > INT64）
    SET sql_text = FORMAT("""      -- GA4 BQ item_params は全値 string_value 格納が基本だが型検出は維持
      CREATE OR REPLACE TEMP TABLE _p0_item_params_raw AS
      SELECT
        ip.key AS param_name,
        CASE
          WHEN ip.value.string_value IS NOT NULL THEN 'STRING'
          WHEN ip.value.double_value IS NOT NULL THEN 'FLOAT64'
          WHEN ip.value.int_value IS NOT NULL THEN 'INT64'
          ELSE 'STRING'
        END AS data_type
      FROM `%s.events_*` e,
      UNNEST(e.items) AS item,
      UNNEST(item.item_params) AS ip
      WHERE e._TABLE_SUFFIX BETWEEN '%s' AND '%s'
        AND ARRAY_LENGTH(e.items) > 0
    """, full_source_path, start_date_str, end_date_str);
    EXECUTE IMMEDIATE sql_text;

    -- 型統一してTEMPテーブルに準備
    SET sql_text = FORMAT("""      CREATE OR REPLACE TEMP TABLE _p0_item_params_new AS
      SELECT param_name,
        CASE
          WHEN COUNTIF(data_type = 'STRING') > 0 THEN 'STRING'
          WHEN COUNTIF(data_type = 'FLOAT64') > 0 THEN 'FLOAT64'
          ELSE 'INT64'
        END AS data_type,
        CONCAT(param_name, '__items') AS micro_column_name,
        CURRENT_TIMESTAMP() AS created_at
      FROM _p0_item_params_raw
      GROUP BY param_name
    """);
    EXECUTE IMMEDIATE sql_text;

    -- MERGE: 既存レコードは型を更新、新規レコードは追加
    SET sql_text = FORMAT("""      MERGE `%s.mst_item_params` AS target
      USING _p0_item_params_new AS source
      ON target.param_name = source.param_name
      WHEN MATCHED THEN
        UPDATE SET
          data_type = source.data_type,
          micro_column_name = source.micro_column_name,
          created_at = source.created_at
      WHEN NOT MATCHED THEN
        INSERT (param_name, data_type, micro_column_name, created_at)
        VALUES (source.param_name, source.data_type, source.micro_column_name, source.created_at)
    """, full_target_path);
    EXECUTE IMMEDIATE sql_text;

    -- 型統一が発生した場合 audit_schema_log に記録
    -- events_involved は固定値 'item_params(all_events)' を使用する
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.audit_schema_log` (column_name, status, reason, original_data_types, events_involved)
      SELECT CONCAT(param_name, '__items'), 'TYPE_UNIFIED_ITEMS',
        'Multiple data types found in item_params, unified by priority rule (STRING > FLOAT64 > INT64)',
        STRING_AGG(DISTINCT data_type ORDER BY data_type),
        'item_params(all_events)'
      FROM _p0_item_params_raw GROUP BY param_name HAVING COUNT(DISTINCT data_type) > 1
    """, full_target_path);

    -- ==================================================================================
    -- ==================================================================================
    IF is_force_reset THEN
      EXECUTE IMMEDIATE FORMAT("""
        DROP TABLE IF EXISTS `%s.micro_items_table`
      """, full_target_path);
    END IF;
    EXECUTE IMMEDIATE FORMAT("""
      CREATE TABLE IF NOT EXISTS `%s.micro_items_table` (
        event_date DATE, event_timestamp DATETIME, event_timestamp_micros INT64,
        event_name STRING,
        user_pseudo_id STRING, user_id STRING, is_identified_user BOOL,
        ga_session_id INT64, pseudonymous_session_id STRING,
        transaction_id STRING, ecommerce_total_value FLOAT64,
        item_index INT64,
        item_id STRING, item_name STRING, item_brand STRING, item_variant STRING,
        item_category STRING, item_category2 STRING, item_category3 STRING,
        item_category4 STRING, item_category5 STRING,
        price FLOAT64, quantity INT64,
        coupon STRING, item_subtotal FLOAT64,
        affiliation STRING, location_id STRING,
        item_list_id STRING, item_list_name STRING, item_list_index STRING,
        promotion_id STRING, promotion_name STRING,
        creative_name STRING, creative_slot STRING,
        event_id STRING, item_event_no INT64,
        device_category STRING, mobile_brand_name STRING,
        operating_system STRING, browser STRING, device_language STRING,
        hostname STRING,
        continent STRING, sub_continent STRING, country STRING, region STRING, city STRING,
        analytics_storage STRING, platform STRING, is_key_event INT64,
        page_location STRING, traffic_source STRING, traffic_medium STRING, traffic_campaign STRING,
        created_at DATETIME, batch_id STRING
      )
      PARTITION BY event_date
      CLUSTER BY user_pseudo_id, item_id, event_name
    """, full_target_path);

    -- micro_items_table へのスキーマ自動拡張（item_params動的カラム）
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE _p0_missing_item_columns AS
      SELECT micro_column_name,
        CASE
          WHEN COUNTIF(data_type = 'STRING') > 0 THEN 'STRING'
          WHEN COUNTIF(data_type = 'FLOAT64') > 0 THEN 'FLOAT64'
          ELSE 'INT64'
        END AS data_type
      FROM `%s.mst_item_params`
      WHERE micro_column_name NOT IN (
        SELECT column_name FROM `%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'micro_items_table'
      )
      GROUP BY micro_column_name
    """, full_target_path, full_target_path);

    SET missing_columns_sql = (
      SELECT STRING_AGG(CONCAT('ADD COLUMN IF NOT EXISTS `', micro_column_name, '` ', data_type), ', ')
      FROM _p0_missing_item_columns
    );

    IF missing_columns_sql IS NOT NULL THEN
      EXECUTE IMMEDIATE FORMAT("ALTER TABLE `%s.micro_items_table` %s", full_target_path, missing_columns_sql);
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `%s.mst_schema_history` (executed_at, column_name, data_type, reason)
        SELECT CURRENT_TIMESTAMP(), micro_column_name, data_type, 'Step 0.7: New item_param added to micro_items_table'
        FROM _p0_missing_item_columns
      """, full_target_path);
    END IF;

    SET phase_0_status = 'SUCCESS';
  EXCEPTION WHEN ERROR THEN
    SET phase_0_status = CONCAT('FAILED: ', @@error.message);
  END;

  -- ==================================================================================
  -- Phase 1: user identity and user-property tables.
  -- ==================================================================================
  IF phase_0_status = 'SUCCESS' THEN
    BEGIN
      -- 変数初期化
      SET p1_start_date = PARSE_DATE('%Y%m%d', start_date_str);
      SET p1_end_date = PARSE_DATE('%Y%m%d', end_date_str);
      SET p1_lookback_start_date = DATE_SUB(p1_start_date, INTERVAL 180 DAY);
      SET p1_lookback_start_suffix = FORMAT_DATE('%Y%m%d', p1_lookback_start_date);
      SET p1_end_date_suffix = FORMAT_DATE('%Y%m%d', p1_end_date);
      SET p1_dynamic_columns_sql = '';
      BEGIN
        SET sql_text = FORMAT("""
          SELECT COUNT(*) > 0
          FROM `%s.%s.INFORMATION_SCHEMA.TABLES`
          WHERE STARTS_WITH(table_name, 'pseudonymous_users_')
            AND SUBSTR(table_name, LENGTH('pseudonymous_users_') + 1)
              BETWEEN '%s' AND '%s'
        """, project_id, source_ds, p1_lookback_start_suffix, p1_end_date_suffix);
        EXECUTE IMMEDIATE sql_text INTO p1_has_pseudonymous_users;
      EXCEPTION WHEN ERROR THEN
        SET p1_has_pseudonymous_users = FALSE;
      END;

      -- Step 1: ID辞書テーブル作成 (dim_user_map_180d)
      SET sql_text = CONCAT("DROP TABLE IF EXISTS `", full_target_path, ".dim_user_map_180d`");
      EXECUTE IMMEDIATE sql_text;

      SET sql_text = CONCAT(
        "CREATE TABLE `", full_target_path, ".dim_user_map_180d` ",
        "PARTITION BY first_touch_date CLUSTER BY user_pseudo_id, user_id AS ",
        "WITH EventUserMap AS ( ",
          "SELECT user_pseudo_id, user_id, TIMESTAMP_MICROS(event_timestamp) AS event_timestamp, ",
            "_TABLE_SUFFIX AS event_date_str, ",
            "ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn ",
          "FROM `", full_source_path, ".events_*` ",
          "WHERE _TABLE_SUFFIX BETWEEN '", p1_lookback_start_suffix, "' AND '", p1_end_date_suffix, "' ",
            "AND user_id IS NOT NULL AND user_id != '' ",
        ") ",
        "SELECT user_pseudo_id, user_id, DATE(event_timestamp, '", batch_timezone, "') AS first_touch_date, ",
          "CURRENT_TIMESTAMP() AS created_at ",
        "FROM EventUserMap WHERE rn = 1"
      );
      EXECUTE IMMEDIATE sql_text;
      SET sql_text = CONCAT("SELECT COUNT(*) FROM `", full_target_path, ".dim_user_map_180d`");
      EXECUTE IMMEDIATE sql_text INTO p1_result_count_1;

      -- Step 1.b: Slot定義マスター作成 (mst_pseudonymous_properties)
      --   user_id は dim_user_map_180d 経由で会員のみ補完（Step 1.c）。
      --   組織系などのカスタムプロパティは非会員にも付く運用のため、
      --   slot key の発見は全ユーザーから行う必要がある。
      IF p1_has_pseudonymous_users THEN
        SET sql_text = CONCAT(
          "CREATE OR REPLACE TABLE `", full_target_path, ".mst_pseudonymous_properties` AS ",
          "WITH SlotDefinitions AS ( ",
            "SELECT prop.key AS slot_key, prop.value.user_property_name AS user_property_name, ",
              "TIMESTAMP_MICROS(prop.value.set_timestamp_micros) AS last_set_timestamp, ",
              "p._TABLE_SUFFIX AS table_suffix, ",
              "ROW_NUMBER() OVER (PARTITION BY prop.key ORDER BY p._TABLE_SUFFIX DESC) AS rn ",
            "FROM `", full_source_path, ".pseudonymous_users_*` p, UNNEST(p.user_properties) AS prop ",
            "WHERE p._TABLE_SUFFIX BETWEEN '", p1_lookback_start_suffix, "' AND '", p1_end_date_suffix, "' ",
              "AND prop.value.user_property_name IS NOT NULL AND prop.value.user_property_name != '' ",
          ") ",
          "SELECT slot_key, user_property_name, last_set_timestamp, DATE(last_set_timestamp, '", batch_timezone, "') AS last_set_date, ",
            "PARSE_DATE('%Y%m%d', table_suffix) AS source_table_date, ",
            "PARSE_DATE('%Y%m%d', '", p1_lookback_start_suffix, "') AS lookback_start_date, ",
            "PARSE_DATE('%Y%m%d', '", p1_end_date_suffix, "') AS lookback_end_date, ",
            "CURRENT_TIMESTAMP() AS created_at ",
          "FROM SlotDefinitions WHERE rn = 1"
        );
      ELSE
        SET sql_text = CONCAT(
          "CREATE OR REPLACE TABLE `", full_target_path, ".mst_pseudonymous_properties` AS ",
          "SELECT ",
            "CAST(NULL AS STRING) AS slot_key, ",
            "CAST(NULL AS STRING) AS user_property_name, ",
            "CAST(NULL AS TIMESTAMP) AS last_set_timestamp, ",
            "CAST(NULL AS DATE) AS last_set_date, ",
            "CAST(NULL AS DATE) AS source_table_date, ",
            "PARSE_DATE('%Y%m%d', '", p1_lookback_start_suffix, "') AS lookback_start_date, ",
            "PARSE_DATE('%Y%m%d', '", p1_end_date_suffix, "') AS lookback_end_date, ",
            "CURRENT_TIMESTAMP() AS created_at ",
          "FROM UNNEST([STRUCT(1 AS _dummy)]) ",
          "WHERE FALSE"
        );
      END IF;
      EXECUTE IMMEDIATE sql_text;
      SET sql_text = CONCAT("SELECT COUNT(*) FROM `", full_target_path, ".mst_pseudonymous_properties`");
      EXECUTE IMMEDIATE sql_text INTO p1_result_count_2;

      -- Step 1.c: 全ユーザー属性ログ (log_pseudonymous_users)
      --   user_id は MasterUsers 経由で会員のみ補完（LEFT JOIN で NULL 許容）。
      --   組織系などのカスタムプロパティは非会員にも付与されるため、
      --   micro_user_table まで正しく伝播させるには全ユーザー取り込みが必須。
      --   MasterUsers の WHERE user_id IS NOT NULL は将来保険として削除
      --   （dim_user_map_180d 自体が user_id IS NOT NULL で構築されているため
      --     現時点では実質的な影響なし）。
      SET sql_text = CONCAT(
        "SELECT STRING_AGG(",
          "CONCAT(",
            "'MAX(CASE WHEN prop.key = ', CHR(39), slot_key, CHR(39), ' THEN prop.value.string_value END) AS `', ",
            "REGEXP_REPLACE(user_property_name, r'[^a-zA-Z0-9_]', '_'), ",
            "'`'",
          "), ', '",
        ") ",
        "FROM `", full_target_path, ".mst_pseudonymous_properties`"
      );
      BEGIN
        EXECUTE IMMEDIATE sql_text INTO p1_dynamic_columns_sql;
      EXCEPTION WHEN ERROR THEN
        SET p1_dynamic_columns_sql = NULL;
      END;

      IF p1_dynamic_columns_sql IS NULL OR p1_dynamic_columns_sql = '' THEN
        SET p1_dynamic_columns_sql = 'NULL AS no_custom_properties';
      END IF;

      IF p1_has_pseudonymous_users THEN
        SET p1_create_table_sql = CONCAT(
          "CREATE OR REPLACE TABLE `", full_target_path, ".log_pseudonymous_users` AS ",
          "WITH MasterUsers AS ( ",
            "SELECT DISTINCT user_pseudo_id, user_id FROM `", full_target_path, ".dim_user_map_180d` ",
          "), ",
          "TargetLogs AS ( ",
            "SELECT p.* EXCEPT(user_id), p.user_id AS ga4_user_id, p._TABLE_SUFFIX AS table_suffix, ",
              "COALESCE(m.user_id, p.user_id) AS resolved_user_id ",
            "FROM `", full_source_path, ".pseudonymous_users_*` p ",
            "LEFT JOIN MasterUsers m ON p.pseudo_user_id = m.user_pseudo_id ",
            "WHERE p._TABLE_SUFFIX BETWEEN '", p1_lookback_start_suffix, "' AND '", p1_end_date_suffix, "' ",
          "), ",
          "LatestTargetLogs AS ( ",
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY pseudo_user_id ORDER BY table_suffix DESC, user_info.last_active_timestamp_micros DESC) AS rn ",
            "FROM TargetLogs ",
          ") ",
          "SELECT ",
            "t.pseudo_user_id AS user_pseudo_id, t.resolved_user_id AS user_id, ",
            "TIMESTAMP_MICROS(t.user_info.last_active_timestamp_micros) AS last_active_timestamp, ",
            "DATE(TIMESTAMP_MICROS(t.user_info.last_active_timestamp_micros), '", batch_timezone, "') AS last_active_date, ",
            "TIMESTAMP_MICROS(t.user_info.user_first_touch_timestamp_micros) AS first_touch_timestamp, ",
            "DATE(TIMESTAMP_MICROS(t.user_info.user_first_touch_timestamp_micros), '", batch_timezone, "') AS first_touch_date, ",
            "t.geo.country, t.geo.region, t.geo.city, ",
            "t.device.category AS device_category, t.device.mobile_brand_name, t.device.operating_system, ",
            p1_dynamic_columns_sql, ", ",
            "CURRENT_TIMESTAMP() AS updated_at, ",
            "PARSE_DATE('%Y%m%d', t.table_suffix) AS source_table_date ",
          "FROM LatestTargetLogs t LEFT JOIN UNNEST(t.user_properties) AS prop WHERE t.rn = 1 ",
          "GROUP BY user_pseudo_id, user_id, last_active_timestamp, last_active_date, ",
            "first_touch_timestamp, first_touch_date, ",
            "country, region, city, device_category, mobile_brand_name, operating_system, ",
            "updated_at, source_table_date"
        );
      ELSE
        SET p1_create_table_sql = CONCAT(
          "CREATE OR REPLACE TABLE `", full_target_path, ".log_pseudonymous_users` AS ",
          "SELECT ",
            "CAST(NULL AS STRING) AS user_pseudo_id, ",
            "CAST(NULL AS STRING) AS user_id, ",
            "CAST(NULL AS TIMESTAMP) AS last_active_timestamp, ",
            "CAST(NULL AS DATE) AS last_active_date, ",
            "CAST(NULL AS TIMESTAMP) AS first_touch_timestamp, ",
            "CAST(NULL AS DATE) AS first_touch_date, ",
            "CAST(NULL AS STRING) AS country, ",
            "CAST(NULL AS STRING) AS region, ",
            "CAST(NULL AS STRING) AS city, ",
            "CAST(NULL AS STRING) AS device_category, ",
            "CAST(NULL AS STRING) AS mobile_brand_name, ",
            "CAST(NULL AS STRING) AS operating_system, ",
            "CAST(NULL AS STRING) AS no_custom_properties, ",
            "CURRENT_TIMESTAMP() AS updated_at, ",
            "CAST(NULL AS DATE) AS source_table_date ",
          "FROM UNNEST([STRUCT(1 AS _dummy)]) ",
          "WHERE FALSE"
        );
      END IF;
      EXECUTE IMMEDIATE p1_create_table_sql;
      SET sql_text = CONCAT("SELECT COUNT(*) FROM `", full_target_path, ".log_pseudonymous_users`");
      EXECUTE IMMEDIATE sql_text INTO p1_result_count_3;

      -- ==================================================================================
      SET sql_text = CONCAT(
        "CREATE TABLE IF NOT EXISTS `", full_target_path, ".log_audience_membership` ( ",
          "user_pseudo_id STRING, user_id STRING, ",
          "audience_id STRING, audience_name STRING, ",
          "membership_start_timestamp TIMESTAMP, membership_expiry_timestamp TIMESTAMP, ",
          "npa BOOL, pseudonymous_session_id STRING, ",
          "source_table_date DATE, created_at TIMESTAMP ",
        ") ",
        "PARTITION BY source_table_date ",
        "CLUSTER BY user_pseudo_id, audience_id"
      );
      EXECUTE IMMEDIATE sql_text;

      IF p1_has_pseudonymous_users THEN
        SET sql_text = CONCAT(
          "INSERT INTO `", full_target_path, ".log_audience_membership` ",
          "SELECT ",
            "p.pseudo_user_id AS user_pseudo_id, ",
            "u.user_id, ",
            "CAST(aud.id AS STRING) AS audience_id, ",
            "aud.name AS audience_name, ",
            "TIMESTAMP_MICROS(aud.membership_start_timestamp_micros) AS membership_start_timestamp, ",
            "TIMESTAMP_MICROS(aud.membership_expiry_timestamp_micros) AS membership_expiry_timestamp, ",
            "aud.npa, ",
            "CONCAT(p.pseudo_user_id, ", CHR(39), "_", CHR(39), ", p._TABLE_SUFFIX) AS pseudonymous_session_id, ",
            "PARSE_DATE(", CHR(39), "%Y%m%d", CHR(39), ", p._TABLE_SUFFIX) AS source_table_date, ",
            "CURRENT_TIMESTAMP() AS created_at ",
          "FROM `", full_source_path, ".pseudonymous_users_*` p, ",
          "UNNEST(p.audiences) AS aud ",
          "LEFT JOIN `", full_target_path, ".dim_user_map_180d` u ON p.pseudo_user_id = u.user_pseudo_id ",
          "WHERE p._TABLE_SUFFIX BETWEEN ", CHR(39), p1_lookback_start_suffix, CHR(39), " AND ", CHR(39), p1_end_date_suffix, CHR(39), " ",
            "AND NOT EXISTS ( ",
              "SELECT 1 FROM `", full_target_path, ".log_audience_membership` existing ",
              "WHERE existing.user_pseudo_id = p.pseudo_user_id ",
                "AND existing.audience_id = CAST(aud.id AS STRING) ",
                "AND existing.membership_start_timestamp = TIMESTAMP_MICROS(aud.membership_start_timestamp_micros) ",
            ")"
        );
        BEGIN
          EXECUTE IMMEDIATE sql_text;
        EXCEPTION WHEN ERROR THEN
          -- audiences フィールドが存在しないか他のエラー: 警告として記録（Phase 1のSUCCESS判定は維持）
          SET p1_audience_warning = CONCAT('audience_warning: ', @@error.message);
        END;
      ELSE
        SET p1_audience_warning = 'audience_warning: pseudonymous_users_* not found; user-data export was skipped';
      END IF;

      -- ==================================================================================
      SET sql_text = CONCAT(
        "CREATE TABLE IF NOT EXISTS `", full_target_path, ".log_user_properties` ( ",
          "user_pseudo_id STRING, user_id STRING, ",
          "property_key STRING, ",
          "property_value_string STRING, property_value_int INT64, property_value_float FLOAT64, ",
          "set_timestamp TIMESTAMP, set_date DATE, ",
          "event_timestamp DATETIME, event_name STRING, ",
          "ga_session_id INT64, pseudonymous_session_id STRING, ",
          "source_event_date DATE, created_at TIMESTAMP ",
        ") ",
        "PARTITION BY set_date ",
        "CLUSTER BY user_pseudo_id, property_key"
      );
      EXECUTE IMMEDIATE sql_text;
    SET sql_text = CONCAT(
        "INSERT INTO `", full_target_path, ".log_user_properties` ",
        "WITH extracted AS ( ",
          "SELECT ",
            "e.user_pseudo_id, ",
            "up.key AS property_key, ",
            "up.value.string_value AS property_value_string, ",
            "up.value.int_value AS property_value_int, ",
            "up.value.float_value AS property_value_float, ",
            "TIMESTAMP_MICROS(up.value.set_timestamp_micros) AS set_timestamp, ",
            "DATE(TIMESTAMP_MICROS(up.value.set_timestamp_micros), ", CHR(39), batch_timezone, CHR(39), ") AS set_date, ",
            "DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ", CHR(39), batch_timezone, CHR(39), ") AS event_timestamp, ",
            "e.event_name, ",
            "(SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = ", CHR(39), "ga_session_id", CHR(39), ") AS ga_session_id, ",
            "PARSE_DATE(", CHR(39), "%Y%m%d", CHR(39), ", e._TABLE_SUFFIX) AS source_event_date, ",
            "ROW_NUMBER() OVER (PARTITION BY e.user_pseudo_id, up.key, up.value.set_timestamp_micros ORDER BY e.event_timestamp DESC) AS _rn ",
          "FROM `", full_source_path, ".events_*` e, ",
          "UNNEST(e.user_properties) AS up ",
          "WHERE e._TABLE_SUFFIX BETWEEN ", CHR(39), start_date_str, CHR(39), " AND ", CHR(39), end_date_str, CHR(39), " ",
            "AND up.value.set_timestamp_micros IS NOT NULL ",
        ") ",
        "SELECT ",
          "ex.user_pseudo_id, ",
          "u.user_id, ",
          "ex.property_key, ",
          "ex.property_value_string, ",
          "ex.property_value_int, ",
          "ex.property_value_float, ",
          "ex.set_timestamp, ",
          "ex.set_date, ",
          "ex.event_timestamp, ",
          "ex.event_name, ",
          "ex.ga_session_id, ",
          "CONCAT(ex.user_pseudo_id, ", CHR(39), "_", CHR(39), ", CAST(ex.ga_session_id AS STRING)) AS pseudonymous_session_id, ",
          "ex.source_event_date, ",
          "CURRENT_TIMESTAMP() AS created_at ",
        "FROM extracted ex ",
        "LEFT JOIN `", full_target_path, ".dim_user_map_180d` u ON ex.user_pseudo_id = u.user_pseudo_id ",
        "WHERE ex._rn = 1 ",
          "AND NOT EXISTS ( ",
            "SELECT 1 FROM `", full_target_path, ".log_user_properties` existing ",
            "WHERE existing.user_pseudo_id = ex.user_pseudo_id ",
              "AND existing.property_key = ex.property_key ",
              "AND existing.set_timestamp = ex.set_timestamp ",
          ")"
      );
      EXECUTE IMMEDIATE sql_text;
    SET sql_text = CONCAT(
        "CREATE OR REPLACE TABLE `", full_target_path, ".mst_user_properties` AS ",
        "SELECT ",
          "property_key, ",
          "CASE ",
            "WHEN COUNTIF(property_value_string IS NOT NULL) > 0 THEN ", CHR(39), "STRING", CHR(39), " ",
            "WHEN COUNTIF(property_value_float IS NOT NULL) > 0 THEN ", CHR(39), "FLOAT64", CHR(39), " ",
            "WHEN COUNTIF(property_value_int IS NOT NULL) > 0 THEN ", CHR(39), "INT64", CHR(39), " ",
            "ELSE ", CHR(39), "STRING", CHR(39), " ",
          "END AS resolved_type, ",
          "COUNT(DISTINCT user_pseudo_id) AS unique_users, ",
          "COUNT(*) AS total_records, ",
          "MIN(set_timestamp) AS first_seen, ",
          "MAX(set_timestamp) AS last_seen ",
        "FROM `", full_target_path, ".log_user_properties` ",
        "GROUP BY property_key"
      );
      EXECUTE IMMEDIATE sql_text;
    IF p1_audience_warning != '' THEN
        SET phase_1_status = CONCAT('SUCCESS (', p1_audience_warning, ')');
      ELSE
        SET phase_1_status = 'SUCCESS';
      END IF;
    EXCEPTION WHEN ERROR THEN
      SET phase_1_status = CONCAT('FAILED: ', @@error.message);
    END;
  ELSE
    SET phase_1_status = 'SKIPPED';
  END IF;

  -- ==================================================================================
  -- Phase 2: event-level source tables.
  -- ==================================================================================
  -- Uses CONCAT-based dynamic SQL and preserves all users through LEFT JOINs.
  -- ==================================================================================
  IF STARTS_WITH(phase_1_status, 'SUCCESS') THEN
    BEGIN
      -- 変数初期化
      SET p2_table_suffix_start = FORMAT_DATE('%Y%m%d', DATE_SUB(PARSE_DATE('%Y%m%d', start_date_str), INTERVAL 1 DAY));
      SET p2_table_suffix_end = end_date_str;
      SET p2_start_date_formatted = FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', start_date_str));
      SET p2_end_date_formatted = FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', end_date_str));
      SET p2_processed_count = 0;
      SET p2_columns_sql = '';
    IF is_force_reset THEN
        EXECUTE IMMEDIATE CONCAT(
          'CREATE OR REPLACE TEMP TABLE _p2_drop_targets AS ',
          'SELECT table_name FROM `', full_target_path, '.INFORMATION_SCHEMA.TABLES` ',
          'WHERE table_name NOT IN (',
            q, 'mst_event_params', q, ',', q, 'mst_event_params_raw', q, ',', q, 'mst_duplicate_params_log', q, ',',
            q, 'mst_pseudonymous_properties', q, ',', q, 'mst_schema_history', q, ',',
            q, 'dim_user_map_180d', q, ',', q, 'log_pseudonymous_users', q, ',', q, 'log_batch_execution', q, ',',
            q, 'log_batch_lock', q, ',',
            q, 'log_integrated_columns', q, ',', q, 'audit_schema_log', q, ',', q, 'micro_user_table', q, ',',
            q, 'micro_items_table', q, ',', q, 'mst_item_params', q, ',',
            q, 'log_audience_membership', q, ',', q, 'log_user_properties', q, ',',
            q, 'mst_user_properties', q, ',',
            q, 'standard_config', q,
          ') ',
          -- Safety hardening: only drop tables that WACA core manages, so
          -- unrelated user tables in the same dataset are preserved even if
          -- TARGET_DATASET is not a dedicated empty dataset. WACA core tables are
          -- either named with a known prefix (mst_/log_/dim_/micro_/audit_) or
          -- are per-event source tables whose names come from mst_event_params
          -- (e.g. page_view, purchase, session_start, custom events).
          'AND (',
            'STARTS_WITH(table_name, ', q, 'mst_', q, ') OR ',
            'STARTS_WITH(table_name, ', q, 'log_', q, ') OR ',
            'STARTS_WITH(table_name, ', q, 'dim_', q, ') OR ',
            'STARTS_WITH(table_name, ', q, 'micro_', q, ') OR ',
            'STARTS_WITH(table_name, ', q, 'audit_', q, ') OR ',
            'table_name IN (SELECT event_name FROM `', full_target_path, '.mst_event_params`)',
          ')'
        );
        FOR drop_rec IN (SELECT table_name FROM _p2_drop_targets)
        DO
          EXECUTE IMMEDIATE CONCAT('DROP TABLE IF EXISTS `', full_target_path, '.', drop_rec.table_name, '`');
        END FOR;
      END IF;

      -- イベントリスト一時テーブル作成 (_p2_event_list)
      EXECUTE IMMEDIATE CONCAT(
        'CREATE OR REPLACE TEMP TABLE _p2_event_list AS ',
        'SELECT DISTINCT event_name FROM `', full_target_path, '.mst_event_params` ORDER BY event_name'
      );
      SET p2_event_count = (SELECT COUNT(*) FROM _p2_event_list);

      -- イベントループ
      FOR event_rec IN (SELECT event_name FROM _p2_event_list)
      DO
        SET p2_current_event_name = event_rec.event_name;
        SET p2_processed_count = p2_processed_count + 1;

        -- 4-A: 動的カラム抽出SQL
        SET p2_sql_for_columns = CONCAT(
          'SELECT STRING_AGG( ',
            'CASE ',
              'WHEN param_name LIKE ', q, '%percent%', q, ' THEN CONCAT( ',
                q, '(SELECT COALESCE(CAST(value.double_value AS FLOAT64), CAST(value.int_value AS FLOAT64)) / 100 FROM UNNEST(event_params) WHERE key = "', q, ', param_name, ', q, '") AS `', q, ', param_name, ', q, '`', q,
              ') ',
              'WHEN data_type = ', q, 'STRING', q, ' THEN CONCAT( ',
                q, '(SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING), CAST(value.double_value AS STRING)) FROM UNNEST(event_params) WHERE key = "', q, ', param_name, ', q, '") AS `', q, ', param_name, ', q, '`', q,
              ') ',
              'WHEN data_type = ', q, 'FLOAT64', q, ' THEN CONCAT( ',
                q, '(SELECT COALESCE(value.double_value, CAST(value.int_value AS FLOAT64)) FROM UNNEST(event_params) WHERE key = "', q, ', param_name, ', q, '") AS `', q, ', param_name, ', q, '`', q,
              ') ',
              'ELSE CONCAT( ',
                q, '(SELECT COALESCE(value.int_value, CAST(value.double_value AS INT64)) FROM UNNEST(event_params) WHERE key = "', q, ', param_name, ', q, '") AS `', q, ', param_name, ', q, '`', q,
              ') ',
            'END, ',
            q, ', ', q,
          ') ',
          'FROM `', full_target_path, '.mst_event_params` ',
          'WHERE event_name = ', q, p2_current_event_name, q, ' ',
            'AND param_name NOT LIKE ', q, 'batch_%', q, ' ',
            'AND param_name != ', q, 'gclid', q, ' ',            CASE WHEN p2_current_event_name = 'session_start' THEN ''
                 ELSE CONCAT('AND param_name NOT LIKE ', q, 'clarity_%', q)
            END
        );
        BEGIN
          EXECUTE IMMEDIATE p2_sql_for_columns INTO p2_columns_sql;
        EXCEPTION WHEN ERROR THEN
          SET p2_columns_sql = NULL;
        END;
        IF p2_columns_sql IS NULL OR p2_columns_sql = '' THEN
          SET p2_columns_sql = 'NULL AS _no_params';
        END IF;
      SET p2_standard_columns = CONCAT(
          -- event_timestamp_micros (INT64 raw value)
          't.event_timestamp AS event_timestamp_micros, ',
          -- device (8 columns)
          't.device.category AS device_category, ',
          't.device.mobile_brand_name, t.device.mobile_model_name, ',
          't.device.operating_system, t.device.operating_system_version, ',
          'COALESCE(t.device.web_info.browser, t.device.browser) AS browser, ',
          't.device.language AS device_language, ',
          't.device.web_info.hostname AS hostname, ',          'LOWER(REGEXP_REPLACE(t.device.web_info.hostname, r', q, '^www\\.', q, ', ', q, q, ')) AS hostname_norm, ',
          -- geo (5 columns)
          't.geo.continent AS continent, ',
          't.geo.sub_continent AS sub_continent, ',
          't.geo.country AS country, t.geo.region, t.geo.city, ',
          -- traffic_source (3 columns)
          't.traffic_source.source AS traffic_source, ',
          't.traffic_source.medium AS traffic_medium, ',
          't.traffic_source.name AS traffic_campaign, ',
          -- collected_traffic_source (3 columns)
          't.collected_traffic_source.manual_source AS collected_source, ',
          't.collected_traffic_source.manual_medium AS collected_medium, ',
          't.collected_traffic_source.manual_campaign_name AS collected_campaign, ',
          -- privacy (1 column)
          't.privacy_info.analytics_storage AS analytics_storage, ',
          -- platform (1 column)
          't.platform, ',          't.collected_traffic_source.gclid AS gclid, ',
          -- is_key_event flag (1 column)
          'CASE WHEN t.event_name = ', q, 'purchase', q, ' THEN 1 ',
            'WHEN STARTS_WITH(t.event_name, ', q, 'CV_', q, ') THEN 1 ',
            'ELSE 0 END AS is_key_event, '
        );
        SET p2_table_exists = FALSE;
        BEGIN
          SET sql_text = CONCAT(
            'SELECT COUNT(*) > 0 FROM `', full_target_path, '.INFORMATION_SCHEMA.TABLES` ',
            'WHERE table_name = ', q, p2_current_event_name, q
          );
          EXECUTE IMMEDIATE sql_text INTO p2_table_exists;
        EXCEPTION WHEN ERROR THEN
          SET p2_table_exists = FALSE;
        END;

        -- 4-C: テーブル作成SQL構築
        --   event_timestamp が完全一致する行をピアとして扱い、同マイクロ秒に発火した
        --   page_view と co-fired イベント (view_item, engagement, scroll 等) が
        IF p2_current_event_name = 'session_start' THEN
          -- session_start: clarity_play_url生成あり
          --   理由: CTE で event_name フィルタすると COUNTIF(event_name='page_view') が
          --         常に 0 になり page_view_id の連番が全イベントで _1 に固定されるバグを解消
          SET p2_create_table_sql = CONCAT(
            'WITH BaseEvents AS ( ',
              'SELECT e.event_timestamp, e.event_name, e.user_pseudo_id, e.event_params, ',
                'e.device, e.geo, e.traffic_source, e.collected_traffic_source, e.privacy_info, e.app_info, e.platform, ',
                '(SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = ', q, 'clarity_project_id', q, ') AS _clarity_pid, ',
                '(SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = ', q, 'clarity_user_id', q, ') AS _clarity_uid, ',
                '(SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = ', q, 'clarity_session_id', q, ') AS _clarity_sid, ',
                '(SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ') AS ga_session_id_val, ',
                'COUNTIF(e.event_name = ', q, 'page_view', q, ') OVER (PARTITION BY e.user_pseudo_id, (SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ') ORDER BY e.event_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS page_number, ',
                'u.user_id ',
              'FROM `', full_source_path, '.events_*` e ',
              'LEFT JOIN `', full_target_path, '.dim_user_map_180d` u ON e.user_pseudo_id = u.user_pseudo_id ',
              'WHERE e._TABLE_SUFFIX BETWEEN ', q, p2_table_suffix_start, q, ' AND ', q, p2_table_suffix_end, q, ' ',
                'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') >= DATETIME(', q, p2_start_date_formatted, ' 00:00:00', q, ') ',
                'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') <= DATETIME(', q, p2_end_date_formatted, ' 23:59:59', q, ') ',
            ') ',
            'SELECT t.user_pseudo_id, t.user_id, t.ga_session_id_val AS ga_session_id, ',
              'CONCAT(t.user_pseudo_id, ', q, '_', q, ', CAST(t.ga_session_id_val AS STRING)) AS pseudonymous_session_id, ',
              'CONCAT(t.user_pseudo_id, ', q, '_', q, ', CAST(t.ga_session_id_val AS STRING), ', q, '_', q, ', CAST(GREATEST(t.page_number, 1) AS STRING)) AS page_view_id, ',
              'DATETIME(TIMESTAMP_MICROS(t.event_timestamp), ', q, batch_timezone, q, ') AS event_timestamp, ',
              'DATE(TIMESTAMP_MICROS(t.event_timestamp), ', q, batch_timezone, q, ') AS event_date, ',
              'CASE WHEN t.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_identified_user, ',
              -- Generate a replay URL only when the Clarity project, user, and
              -- session identifiers are all present in the event data. No fallback
              -- project id is used: if the project is absent, the URL is NULL.
              'CASE WHEN t._clarity_pid IS NOT NULL AND t._clarity_uid IS NOT NULL AND t._clarity_sid IS NOT NULL THEN ',
                'CONCAT(', q, 'https://clarity.microsoft.com/player/', q, ', t._clarity_pid, ', q, '/', q, ', t._clarity_uid, ', q, '/', q, ', t._clarity_sid) ',
                'ELSE CAST(NULL AS STRING) END AS clarity_play_url, ',
              p2_standard_columns,
              p2_columns_sql, ', ',
              q, batch_id, q, ' AS batch_id ',
            'FROM BaseEvents t ',
            'WHERE t.event_name = ', q, p2_current_event_name, q
          );
        ELSEIF p2_current_event_name = 'page_view' THEN
        SET p2_create_table_sql = CONCAT(
            'WITH BaseEvents AS ( ',
              'SELECT e.event_timestamp, e.event_name, e.user_pseudo_id, e.event_params, ',
                'e.device, e.geo, e.traffic_source, e.collected_traffic_source, e.privacy_info, e.app_info, e.platform, ',
                '(SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ') AS ga_session_id_val, ',
                'COUNTIF(e.event_name = ', q, 'page_view', q, ') OVER (PARTITION BY e.user_pseudo_id, (SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ') ORDER BY e.event_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS page_number, ',
                'u.user_id ',
              'FROM `', full_source_path, '.events_*` e ',
              'LEFT JOIN `', full_target_path, '.dim_user_map_180d` u ON e.user_pseudo_id = u.user_pseudo_id ',
              'WHERE e._TABLE_SUFFIX BETWEEN ', q, p2_table_suffix_start, q, ' AND ', q, p2_table_suffix_end, q, ' ',
                'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') >= DATETIME(', q, p2_start_date_formatted, ' 00:00:00', q, ') ',
                'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') <= DATETIME(', q, p2_end_date_formatted, ' 23:59:59', q, ') ',
            ') ',
            'SELECT t.user_pseudo_id, t.user_id, t.ga_session_id_val AS ga_session_id, ',
              'CONCAT(t.user_pseudo_id, ', q, '_', q, ', CAST(t.ga_session_id_val AS STRING)) AS pseudonymous_session_id, ',
              'CONCAT(t.user_pseudo_id, ', q, '_', q, ', CAST(t.ga_session_id_val AS STRING), ', q, '_', q, ', CAST(GREATEST(t.page_number, 1) AS STRING)) AS page_view_id, ',
              'DATETIME(TIMESTAMP_MICROS(t.event_timestamp), ', q, batch_timezone, q, ') AS event_timestamp, ',
              'DATE(TIMESTAMP_MICROS(t.event_timestamp), ', q, batch_timezone, q, ') AS event_date, ',
              'CASE WHEN t.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_identified_user, ',
              p2_standard_columns,
              p2_columns_sql, ', ',
              q, batch_id, q, ' AS batch_id ',
            'FROM BaseEvents t ',
            'WHERE t.event_name = ', q, p2_current_event_name, q
          );
        ELSE          --   全イベントが _1 に固定されるバグ。CTE で全イベント走査→外側で event_name フィルタ
          SET p2_create_table_sql = CONCAT(
            'WITH BaseEvents AS ( ',
              'SELECT e.event_timestamp, e.event_name, e.user_pseudo_id, e.event_params, ',
                'e.device, e.geo, e.traffic_source, e.collected_traffic_source, e.privacy_info, e.app_info, e.platform, ',
                '(SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ') AS ga_session_id_val, ',
                'COUNTIF(e.event_name = ', q, 'page_view', q, ') OVER (PARTITION BY e.user_pseudo_id, (SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ') ORDER BY e.event_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS page_number, ',
                'u.user_id ',
              'FROM `', full_source_path, '.events_*` e ',
              'LEFT JOIN `', full_target_path, '.dim_user_map_180d` u ON e.user_pseudo_id = u.user_pseudo_id ',
              'WHERE e._TABLE_SUFFIX BETWEEN ', q, p2_table_suffix_start, q, ' AND ', q, p2_table_suffix_end, q, ' ',
                'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') >= DATETIME(', q, p2_start_date_formatted, ' 00:00:00', q, ') ',
                'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') <= DATETIME(', q, p2_end_date_formatted, ' 23:59:59', q, ') ',
            ') ',
            'SELECT t.user_pseudo_id, t.user_id, t.ga_session_id_val AS ga_session_id, ',
              'CONCAT(t.user_pseudo_id, ', q, '_', q, ', CAST(t.ga_session_id_val AS STRING)) AS pseudonymous_session_id, ',
              'CONCAT(t.user_pseudo_id, ', q, '_', q, ', CAST(t.ga_session_id_val AS STRING), ', q, '_', q, ', CAST(GREATEST(t.page_number, 1) AS STRING)) AS page_view_id, ',
              'DATETIME(TIMESTAMP_MICROS(t.event_timestamp), ', q, batch_timezone, q, ') AS event_timestamp, ',
              'DATE(TIMESTAMP_MICROS(t.event_timestamp), ', q, batch_timezone, q, ') AS event_date, ',
              'CASE WHEN t.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_identified_user, ',
              p2_standard_columns,
              p2_columns_sql, ', ',
              q, batch_id, q, ' AS batch_id ',
            'FROM BaseEvents t ',
            'WHERE t.event_name = ', q, p2_current_event_name, q
          );
        END IF;
      IF p2_table_exists THEN
          EXECUTE IMMEDIATE CONCAT(
            'ALTER TABLE `', full_target_path, '.', p2_current_event_name, '` ',
            'ADD COLUMN IF NOT EXISTS batch_id STRING'
          );
          -- 該当日のデータを削除
          SET sql_text = CONCAT(
            'DELETE FROM `', full_target_path, '.', p2_current_event_name, '` ',
            'WHERE event_date BETWEEN ', q, p2_start_date_formatted, q, ' AND ', q, p2_end_date_formatted, q
          );
          BEGIN
            EXECUTE IMMEDIATE sql_text;
          EXCEPTION WHEN ERROR THEN
            -- テーブルにevent_dateがない場合（古いスキーマ）はDROPして再作成
            SET p2_table_exists = FALSE;
          END;
        END IF;

        IF p2_table_exists THEN
          -- INSERT INTO 既存テーブル（スキーマ不一致時はCREATE OR REPLACEにフォールバック）
          SET sql_text = CONCAT('INSERT INTO `', full_target_path, '.', p2_current_event_name, '` ', p2_create_table_sql);
          BEGIN
            EXECUTE IMMEDIATE sql_text;
          EXCEPTION WHEN ERROR THEN
            -- スキーマ不一致の場合はDROPして再作成
            SET sql_text = CONCAT('CREATE OR REPLACE TABLE `', full_target_path, '.', p2_current_event_name, '` AS ', p2_create_table_sql);
            EXECUTE IMMEDIATE sql_text;
          END;
        ELSE
          -- CREATE TABLE（初回 or スキーマ変更時）
          SET sql_text = CONCAT('CREATE OR REPLACE TABLE `', full_target_path, '.', p2_current_event_name, '` AS ', p2_create_table_sql);
          EXECUTE IMMEDIATE sql_text;
        END IF;
      END FOR;

      SET phase_2_status = 'SUCCESS';
    EXCEPTION WHEN ERROR THEN
      SET phase_2_status = CONCAT('FAILED: ', @@error.message);
    END;
  ELSE
    SET phase_2_status = 'SKIPPED';
  END IF;

  -- ==================================================================================
  -- Phase 3A: micro_user_table generation.
  -- ==================================================================================
  -- Daily batches should use is_incremental=TRUE. A non-incremental run rebuilds
  -- micro_user_table.
  -- ==================================================================================
  IF phase_2_status = 'SUCCESS' THEN
    BEGIN
      -- パラメータ変換
      SET p3_is_incremental = is_incremental;
      SET p3_start_date = PARSE_DATE('%Y%m%d', start_date_str);
      SET p3_end_date = PARSE_DATE('%Y%m%d', end_date_str);

      SET p3_result_count = 0;
      SET p3_processed_events = 0;

      IF p3_is_incremental THEN
        SET p3_date_filter = CONCAT(" AND e.event_date BETWEEN DATE('", CAST(p3_start_date AS STRING), "') AND DATE('", CAST(p3_end_date AS STRING), "')");
      ELSE
        SET p3_date_filter = "";
      END IF;
      -- value → unique（イベント別分離・常時）
      -- custom param + custom event → unique（イベント別分離）
      -- custom param + default event → common（統合・サフィックスなし）
      -- auto/enhanced/recommended → common（統合）
      SET sql_text = CONCAT(
        "CREATE OR REPLACE TEMP TABLE _param_class AS ",
        "SELECT param_name, micro_column_name, event_name, param_category, event_category, ",
          "COUNT(DISTINCT event_name) OVER (PARTITION BY param_name) AS event_count, ",
          "CASE ",
            "WHEN param_name = 'value' THEN 'unique' ",
            "WHEN param_category = 'custom' AND event_category = 'custom' THEN 'unique' ",
            "ELSE 'common' ",
          "END AS ptype ",
        "FROM `", full_target_path, ".mst_event_params` ",
        "WHERE param_name NOT LIKE '", p_exclude_prefix_1, "%' ",
        "AND param_name NOT LIKE '", p_exclude_prefix_2, "%' ",
        "AND param_name != '", p_exclude_key, "'"
      );
      EXECUTE IMMEDIATE sql_text;

      -- 4. 最終カラム名マッピング（micro_column_name from mst_event_params を使用）
      SET sql_text = CONCAT(
        "CREATE OR REPLACE TEMP TABLE _final_columns AS ",
        "SELECT DISTINCT micro_column_name AS final_col_name, ",
          "param_name AS source_param, ",
          "CASE WHEN ptype = 'common' THEN NULL ELSE event_name END AS source_event, ptype ",
        "FROM _param_class"
      );
      EXECUTE IMMEDIATE sql_text;

      -- 5. CVセッション特定（STARTS_WITH方式）
      SET sql_text = CONCAT(
        "CREATE OR REPLACE TEMP TABLE _cv_events AS ",
        "SELECT table_name AS event_name ",
        "FROM `", full_target_path, ".INFORMATION_SCHEMA.TABLES` ",
        "WHERE STARTS_WITH(LOWER(table_name), 'cv_') OR LOWER(table_name) = 'purchase'"
      );
      EXECUTE IMMEDIATE sql_text;

      SET sql_text = CONCAT(
        "CREATE OR REPLACE TEMP TABLE _key_sessions AS ",
        "SELECT DISTINCT user_pseudo_id, ga_session_id FROM ( "
      );
      SET p3_processed_events = 0;

      FOR cv_rec IN (SELECT event_name FROM _cv_events)
      DO
        IF p3_processed_events > 0 THEN
          SET sql_text = CONCAT(sql_text, " UNION ALL ");
        END IF;
        IF p3_is_incremental THEN
          SET sql_text = CONCAT(sql_text,
            "SELECT user_pseudo_id, ga_session_id FROM `", full_target_path, ".", cv_rec.event_name, "` ",
            "WHERE event_date BETWEEN DATE('", CAST(p3_start_date AS STRING), "') AND DATE('", CAST(p3_end_date AS STRING), "')"
          );
        ELSE
          SET sql_text = CONCAT(sql_text,
            "SELECT user_pseudo_id, ga_session_id FROM `", full_target_path, ".", cv_rec.event_name, "`"
          );
        END IF;
        SET p3_processed_events = p3_processed_events + 1;
      END FOR;

      IF p3_processed_events = 0 THEN
        -- No key-event tables (no cv_* / purchase yet): build an empty set.
        -- A bare SELECT cannot carry WHERE, so select from a one-row UNNEST.
        SET sql_text = CONCAT(sql_text, "SELECT CAST(NULL AS STRING) AS user_pseudo_id, CAST(NULL AS INT64) AS ga_session_id FROM UNNEST([STRUCT(1 AS _dummy)]) WHERE FALSE");
      END IF;
      SET sql_text = CONCAT(sql_text, ")");
      EXECUTE IMMEDIATE sql_text;
      SET p3_processed_events = 0;

      -- 6. 動的スキーマ生成（型競合解決）
      SET sql_text = CONCAT(
        "CREATE OR REPLACE TEMP TABLE _column_types AS ",
        "WITH all_cols AS ( ",
          "SELECT column_name, data_type FROM `", full_target_path, ".INFORMATION_SCHEMA.COLUMNS` ",
          "WHERE table_name IN (SELECT DISTINCT event_name FROM `", full_target_path, ".mst_event_params`) ",
            "AND column_name NOT IN ('event_date','event_timestamp','event_timestamp_micros','event_name','user_pseudo_id',",
                "'ga_session_id','user_id','pseudonymous_session_id','page_view_id','clarity_play_url',",
                "'device_category','mobile_brand_name','mobile_model_name','operating_system',",
                "'operating_system_version','browser','device_language','hostname','hostname_norm',",
                "'continent','sub_continent','country','region','city',",
                "'traffic_source','traffic_medium','traffic_campaign',",
                "'collected_source','collected_medium','collected_campaign',",
                "'analytics_storage','platform','gclid','is_key_event','is_identified_user','_rn') ",
        ") ",
        "SELECT fc.final_col_name, fc.source_param, fc.source_event, fc.ptype, ",
          "CASE WHEN COUNT(DISTINCT ac.data_type) > 1 THEN 'STRING' ",
            "WHEN MAX(ac.data_type) LIKE 'INT%' THEN 'INT64' ",
            "WHEN MAX(ac.data_type) LIKE 'FLOAT%' THEN 'FLOAT64' ELSE 'STRING' ",
          "END AS unified_type ",
        "FROM _final_columns fc LEFT JOIN all_cols ac ON fc.source_param = ac.column_name ",
        "GROUP BY fc.final_col_name, fc.source_param, fc.source_event, fc.ptype"
      );
      EXECUTE IMMEDIATE sql_text;

      -- 7. Slotカラム追加
      SET sql_text = CONCAT(
        "INSERT INTO _column_types (final_col_name, source_param, source_event, ptype, unified_type) ",
        "SELECT user_property_name, user_property_name, NULL, 'slot', 'STRING' ",
        "FROM `", full_target_path, ".mst_pseudonymous_properties`"
      );
      EXECUTE IMMEDIATE sql_text;

      -- 8. DDLカラム定義（ORDER BY final_col_name）
      SET sql_text = "SELECT STRING_AGG(CONCAT('`', final_col_name, '` ', unified_type), ', ' ORDER BY final_col_name) FROM _column_types";
      EXECUTE IMMEDIATE sql_text INTO p3_dynamic_schema_sql;
      IF p3_dynamic_schema_sql IS NULL OR p3_dynamic_schema_sql = '' THEN
        SET p3_dynamic_schema_sql = '`_placeholder` STRING';
      END IF;
      SET sql_text = "SELECT STRING_AGG(CONCAT('`', final_col_name, '`'), ', ' ORDER BY final_col_name) FROM _column_types";
      EXECUTE IMMEDIATE sql_text INTO p3_dynamic_col_names_sql;
      IF p3_dynamic_col_names_sql IS NULL OR p3_dynamic_col_names_sql = '' THEN
        SET p3_dynamic_col_names_sql = '`_placeholder`';
      END IF;
      -- INSERT文のフルカラムリスト（固定プレフィックス + 動的カラム + 固定サフィックス）
      SET p3_insert_col_list = CONCAT(
        'event_date, event_timestamp, event_timestamp_micros, event_name, ',
        'user_pseudo_id, user_id, is_identified_user, ga_session_id, ',
        'pseudonymous_session_id, page_view_id, is_key_event, ',
        'event_id, session_event_no, user_event_no, ',
        'clarity_play_url, ',
        'device_category, mobile_brand_name, mobile_model_name, ',
        'operating_system, operating_system_version, browser, ',
        'device_language, hostname, hostname_norm, ',
        'prev_hostname_norm, next_hostname_norm, is_cross_domain_hop, ',
        'continent, sub_continent, country, region, city, ',
        'traffic_source, traffic_medium, traffic_campaign, ',
        'collected_source, collected_medium, collected_campaign, ',
        'analytics_storage, platform, gclid, ',
        'prev_event_name, seconds_from_prev_event, ',
        'max_scroll_percent, ',
        p3_dynamic_col_names_sql, ', ',
        'last_active_timestamp, last_active_date, ',
        'first_touch_timestamp, first_touch_date, ',
        'updated_at, source_table_date, batch_id'
      );

      -- 9. テーブル作成 or 増分DELETE
      SET sql_text = CONCAT(
        "SELECT COUNT(*) > 0 FROM `", full_target_path, ".INFORMATION_SCHEMA.TABLES` ",
        "WHERE table_name = 'micro_user_table'"
      );
      BEGIN
        EXECUTE IMMEDIATE sql_text INTO p3_table_exists;
      EXCEPTION WHEN ERROR THEN
        SET p3_table_exists = FALSE;
      END;
      IF p3_is_incremental AND p3_table_exists THEN
        SET p3_schema_ok = FALSE;
        BEGIN
          SET sql_text = CONCAT(
            -- 固定カラム 51 個すべてが揃っている場合のみ増分 INSERT を許可する。
            -- Phase 0 の CREATE TABLE IF NOT EXISTS が作る最小スタブ（21 列）や旧版の
            -- テーブルは pseudonymous_session_id 等を欠くため、ここで CREATE OR REPLACE に落とす。
            "SELECT COUNTIF(column_name IN (",
              "'event_date','event_timestamp','event_timestamp_micros','event_name',",
              "'user_pseudo_id','user_id','is_identified_user','ga_session_id',",
              "'pseudonymous_session_id','page_view_id','is_key_event',",
              "'event_id','session_event_no','user_event_no','clarity_play_url',",
              "'device_category','mobile_brand_name','mobile_model_name',",
              "'operating_system','operating_system_version','browser',",
              "'device_language','hostname','hostname_norm',",
              "'prev_hostname_norm','next_hostname_norm','is_cross_domain_hop',",
              "'continent','sub_continent','country','region','city',",
              "'traffic_source','traffic_medium','traffic_campaign',",
              "'collected_source','collected_medium','collected_campaign',",
              "'analytics_storage','platform','gclid',",
              "'prev_event_name','seconds_from_prev_event','max_scroll_percent',",
              "'last_active_timestamp','last_active_date',",
              "'first_touch_timestamp','first_touch_date',",
              "'updated_at','source_table_date','batch_id'",
            ")) = 51 ",
            "FROM `", full_target_path, ".INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'micro_user_table'"
          );
          EXECUTE IMMEDIATE sql_text INTO p3_schema_ok;
        EXCEPTION WHEN ERROR THEN
          SET p3_schema_ok = FALSE;
        END;
        IF NOT p3_schema_ok THEN
          SET p3_table_exists = FALSE;  -- スキーマ不足 → CREATE OR REPLACE へフォールバック
        END IF;
      END IF;

      IF NOT p3_is_incremental OR NOT p3_table_exists THEN
        -- 全量更新 or テーブル不在 or スキーマ不足: CREATE OR REPLACE
        SET sql_text = CONCAT(
          "CREATE OR REPLACE TABLE `", full_target_path, ".micro_user_table` ( ",
            "event_date DATE, event_timestamp DATETIME, event_timestamp_micros INT64, event_name STRING, ",
            "user_pseudo_id STRING, user_id STRING, is_identified_user BOOL, ga_session_id INT64, ",
            "pseudonymous_session_id STRING, page_view_id STRING, is_key_event INT64, ",
            "event_id STRING, session_event_no INT64, user_event_no INT64, ",
            "clarity_play_url STRING, ",
            "device_category STRING, mobile_brand_name STRING, mobile_model_name STRING, ",
            "operating_system STRING, operating_system_version STRING, browser STRING, ",
            "device_language STRING, hostname STRING, hostname_norm STRING, ",
            "prev_hostname_norm STRING, next_hostname_norm STRING, is_cross_domain_hop BOOL, ",
            "continent STRING, sub_continent STRING, country STRING, region STRING, city STRING, ",
            "traffic_source STRING, traffic_medium STRING, traffic_campaign STRING, ",
            "collected_source STRING, collected_medium STRING, collected_campaign STRING, ",
            "analytics_storage STRING, platform STRING, gclid STRING, ",
            "prev_event_name STRING, seconds_from_prev_event INT64, ",
            "max_scroll_percent INT64, ",
            p3_dynamic_schema_sql, ", ",
            "last_active_timestamp TIMESTAMP, last_active_date DATE, ",
            "first_touch_timestamp TIMESTAMP, first_touch_date DATE, ",
            "updated_at TIMESTAMP, source_table_date DATE, ",
            "batch_id STRING ",
          ") PARTITION BY event_date CLUSTER BY user_pseudo_id, user_id"
        );
        EXECUTE IMMEDIATE sql_text;
      ELSE
        EXECUTE IMMEDIATE CONCAT(
          "ALTER TABLE `", full_target_path, ".micro_user_table` ",
          "ADD COLUMN IF NOT EXISTS batch_id STRING"
        );
        SET sql_text = CONCAT(
          "DELETE FROM `", full_target_path, ".micro_user_table` ",
          "WHERE event_date BETWEEN DATE('", CAST(p3_start_date AS STRING), "') AND DATE('", CAST(p3_end_date AS STRING), "')"
        );
        EXECUTE IMMEDIATE sql_text;
      END IF;

      -- 10. イベントリスト（scroll除外）- _p3_event_list
      SET sql_text = CONCAT(
        "CREATE OR REPLACE TEMP TABLE _p3_event_list AS ",
        "SELECT DISTINCT event_name FROM `", full_target_path, ".mst_event_params` ",
        "WHERE event_name != 'scroll' ",
        "ORDER BY CASE WHEN event_name = 'page_view' THEN 0 ELSE 1 END, event_name"
      );
      EXECUTE IMMEDIATE sql_text;

      -- 11. Fill: 各イベントINSERT（scroll以外）
      FOR ev IN (SELECT event_name FROM _p3_event_list)
      DO
        SET p3_current_event = ev.event_name;
        SET p3_processed_events = p3_processed_events + 1;

        -- SELECT句生成
        SET sql_text = CONCAT(
          "CREATE OR REPLACE TEMP TABLE _current_select AS ",
          "WITH event_cols AS ( ",
            "SELECT column_name FROM `", full_target_path, ".INFORMATION_SCHEMA.COLUMNS` ",
            "WHERE table_name = '", p3_current_event, "' ",
          ") ",
          "SELECT ct.final_col_name, ",
            "CASE ",
              "WHEN ct.ptype = 'common' AND ct.source_param IN (SELECT column_name FROM event_cols) ",
                "THEN CONCAT('CAST(e.`', ct.source_param, '` AS ', ct.unified_type, ')') ",
              "WHEN ct.ptype = 'unique' AND ct.source_event = '", p3_current_event, "' ",
                "THEN CONCAT('CAST(e.`', ct.source_param, '` AS ', ct.unified_type, ')') ",
              "WHEN ct.ptype = 'slot' THEN CONCAT('CAST(NULL AS ', ct.unified_type, ')') ",
              "ELSE CONCAT('CAST(NULL AS ', ct.unified_type, ')') ",
            "END AS select_expr ",
          "FROM _column_types ct"
        );
        EXECUTE IMMEDIATE sql_text;

        -- ORDER BY final_col_name
        SET sql_text = "SELECT STRING_AGG(select_expr, ', ' ORDER BY final_col_name) FROM _current_select";
        EXECUTE IMMEDIATE sql_text INTO p3_insert_select_sql;
        IF p3_insert_select_sql IS NULL OR p3_insert_select_sql = '' THEN
          SET p3_insert_select_sql = 'NULL';
        END IF;

        -- INSERT実行
        IF p3_current_event = 'session_start' THEN
          -- session_start: clarity_play_urlを直接取得（session_startテーブルに格納済み）
          SET sql_text = CONCAT(
            "INSERT INTO `", full_target_path, ".micro_user_table` (", p3_insert_col_list, ") ",
            "SELECT e.event_date, e.event_timestamp, e.event_timestamp_micros, '", p3_current_event, "', ",
              "e.user_pseudo_id, COALESCE(e.user_id, u.user_id), e.is_identified_user, e.ga_session_id, ",
              "e.pseudonymous_session_id, e.page_view_id, ",
              "CASE WHEN ks.ga_session_id IS NOT NULL THEN 1 ELSE 0 END, ",
              "NULL, NULL, NULL, ",  -- event_id, session_event_no, user_event_no (computed post-INSERT)
              "e.clarity_play_url, ",
              "e.device_category, e.mobile_brand_name, e.mobile_model_name, e.operating_system, e.operating_system_version, e.browser, ",
              "e.device_language, e.hostname, e.hostname_norm, ",
              "NULL, NULL, NULL, ",  -- prev_hostname_norm, next_hostname_norm, is_cross_domain_hop (computed post-INSERT)
              "e.continent, e.sub_continent, e.country, e.region, e.city, ",
              "e.traffic_source, e.traffic_medium, e.traffic_campaign, ",
              "e.collected_source, e.collected_medium, e.collected_campaign, ",
              "e.analytics_storage, e.platform, e.gclid, ",
              "NULL, NULL, ",  -- prev_event_name, seconds_from_prev_event (computed post-INSERT)
              "NULL, ", p3_insert_select_sql, ", NULL, NULL, NULL, NULL, NULL, NULL, '", batch_id, "' ",
            "FROM `", full_target_path, ".", p3_current_event, "` e ",
            "LEFT JOIN `", full_target_path, ".log_pseudonymous_users` u ON e.user_pseudo_id = u.user_pseudo_id ",
            "LEFT JOIN _key_sessions ks ON e.user_pseudo_id = ks.user_pseudo_id AND e.ga_session_id = ks.ga_session_id ",
            "WHERE 1=1", p3_date_filter
          );
        ELSEIF p3_current_event = 'page_view' THEN
          -- page_view: clarity_play_urlはsession_startからJOINで取得、デバイス情報は自身から
          SET sql_text = CONCAT(
            "INSERT INTO `", full_target_path, ".micro_user_table` (", p3_insert_col_list, ") ",
            "SELECT e.event_date, e.event_timestamp, e.event_timestamp_micros, '", p3_current_event, "', ",
              "e.user_pseudo_id, COALESCE(e.user_id, u.user_id), e.is_identified_user, e.ga_session_id, ",
              "e.pseudonymous_session_id, e.page_view_id, ",
              "CASE WHEN ks.ga_session_id IS NOT NULL THEN 1 ELSE 0 END, ",
              "NULL, NULL, NULL, ",  -- event_id, session_event_no, user_event_no
              "ss.clarity_play_url, ",
              "e.device_category, e.mobile_brand_name, e.mobile_model_name, e.operating_system, e.operating_system_version, e.browser, ",
              "e.device_language, e.hostname, e.hostname_norm, ",
              "NULL, NULL, NULL, ",  -- prev_hostname_norm, next_hostname_norm, is_cross_domain_hop
              "e.continent, e.sub_continent, e.country, e.region, e.city, ",
              "e.traffic_source, e.traffic_medium, e.traffic_campaign, ",
              "e.collected_source, e.collected_medium, e.collected_campaign, ",
              "e.analytics_storage, e.platform, e.gclid, ",
              "NULL, NULL, ",  -- prev_event_name, seconds_from_prev_event
              "NULL, ", p3_insert_select_sql, ", NULL, NULL, NULL, NULL, NULL, NULL, '", batch_id, "' ",
            "FROM `", full_target_path, ".", p3_current_event, "` e ",
            "LEFT JOIN (SELECT user_pseudo_id, ga_session_id, ANY_VALUE(clarity_play_url) AS clarity_play_url FROM `", full_target_path, ".session_start` GROUP BY user_pseudo_id, ga_session_id) ss ON e.user_pseudo_id = ss.user_pseudo_id AND e.ga_session_id = ss.ga_session_id ",
            "LEFT JOIN `", full_target_path, ".log_pseudonymous_users` u ON e.user_pseudo_id = u.user_pseudo_id ",
            "LEFT JOIN _key_sessions ks ON e.user_pseudo_id = ks.user_pseudo_id AND e.ga_session_id = ks.ga_session_id ",
            "WHERE 1=1", p3_date_filter
          );
        ELSE          SET sql_text = CONCAT(
            "INSERT INTO `", full_target_path, ".micro_user_table` (", p3_insert_col_list, ") ",
            "SELECT e.event_date, e.event_timestamp, e.event_timestamp_micros, '", p3_current_event, "', ",
              "e.user_pseudo_id, COALESCE(e.user_id, u.user_id), e.is_identified_user, e.ga_session_id, ",
              "e.pseudonymous_session_id, e.page_view_id, ",
              "CASE WHEN ks.ga_session_id IS NOT NULL THEN 1 ELSE 0 END, ",
              "NULL, NULL, NULL, ",  -- event_id, session_event_no, user_event_no
              "ss.clarity_play_url, ",
              "e.device_category, e.mobile_brand_name, e.mobile_model_name, e.operating_system, e.operating_system_version, e.browser, ",
              "e.device_language, e.hostname, e.hostname_norm, ",
              "NULL, NULL, NULL, ",  -- prev_hostname_norm, next_hostname_norm, is_cross_domain_hop
              "e.continent, e.sub_continent, e.country, e.region, e.city, ",
              "e.traffic_source, e.traffic_medium, e.traffic_campaign, ",
              "e.collected_source, e.collected_medium, e.collected_campaign, ",
              "e.analytics_storage, e.platform, e.gclid, ",
              "NULL, NULL, ",  -- prev_event_name, seconds_from_prev_event
              "NULL, ", p3_insert_select_sql, ", NULL, NULL, NULL, NULL, NULL, NULL, '", batch_id, "' ",
            "FROM `", full_target_path, ".", p3_current_event, "` e ",
            "LEFT JOIN (SELECT user_pseudo_id, ga_session_id, ANY_VALUE(clarity_play_url) AS clarity_play_url FROM `", full_target_path, ".session_start` GROUP BY user_pseudo_id, ga_session_id) ss ON e.user_pseudo_id = ss.user_pseudo_id AND e.ga_session_id = ss.ga_session_id ",
            "LEFT JOIN `", full_target_path, ".log_pseudonymous_users` u ON e.user_pseudo_id = u.user_pseudo_id ",
            "LEFT JOIN _key_sessions ks ON e.user_pseudo_id = ks.user_pseudo_id AND e.ga_session_id = ks.ga_session_id ",
            "WHERE 1=1", p3_date_filter
          );
        END IF;

        BEGIN
          EXECUTE IMMEDIATE sql_text;
          SET p3_success_count = p3_success_count + 1;
        EXCEPTION WHEN ERROR THEN
          SET p3_failed_events = ARRAY_CONCAT(p3_failed_events, [p3_current_event]);
          SET p3_failed_errors = ARRAY_CONCAT(p3_failed_errors,
            [CONCAT(p3_current_event, ': ', SUBSTR(@@error.message, 1, 500))]);
          IF p3_first_error_msg IS NULL THEN
            SET p3_first_error_msg = CONCAT(p3_current_event, ': ', SUBSTR(@@error.message, 1, 500));
          END IF;
        END;
      END FOR;

      -- 12. スクロール最大値 UPDATE
      SET p3_scroll_exists = FALSE;
      SET sql_text = CONCAT(
        "SELECT COUNT(*) > 0 FROM `", full_target_path, ".INFORMATION_SCHEMA.TABLES` WHERE table_name = 'scroll'"
      );
      BEGIN
        EXECUTE IMMEDIATE sql_text INTO p3_scroll_exists;
      EXCEPTION WHEN ERROR THEN
        SET p3_scroll_exists = FALSE;
      END;

      IF p3_scroll_exists THEN
        IF p3_is_incremental THEN
          SET sql_text = CONCAT(
            "UPDATE `", full_target_path, ".micro_user_table` m SET m.max_scroll_percent = s.max_pct ",
            "FROM (SELECT page_view_id, MAX(CAST(percent_scrolled AS INT64)) AS max_pct ",
              "FROM `", full_target_path, ".scroll` WHERE page_view_id IS NOT NULL ",
              "AND event_date BETWEEN DATE('", CAST(p3_start_date AS STRING), "') AND DATE('", CAST(p3_end_date AS STRING), "') ",
              "GROUP BY page_view_id) s ",
            "WHERE m.page_view_id = s.page_view_id AND m.event_name = 'page_view'"
          );
        ELSE
          SET sql_text = CONCAT(
            "UPDATE `", full_target_path, ".micro_user_table` m SET m.max_scroll_percent = s.max_pct ",
            "FROM (SELECT page_view_id, MAX(CAST(percent_scrolled AS INT64)) AS max_pct ",
              "FROM `", full_target_path, ".scroll` WHERE page_view_id IS NOT NULL GROUP BY page_view_id) s ",
            "WHERE m.page_view_id = s.page_view_id AND m.event_name = 'page_view'"
          );
        END IF;
        EXECUTE IMMEDIATE sql_text;
      END IF;
      SET sql_text = CONCAT(
        "CREATE OR REPLACE TABLE `", full_target_path, ".micro_user_table` ",
        "PARTITION BY event_date ",
        "CLUSTER BY user_pseudo_id, user_id ",
        "AS WITH deduped AS ( ",
          "SELECT *, ROW_NUMBER() OVER( ",
            "PARTITION BY user_pseudo_id, event_timestamp_micros, event_name, COALESCE(ga_session_id, -1) ",
            "ORDER BY page_view_id ",
          ") AS _dedup_rn ",
          "FROM `", full_target_path, ".micro_user_table` ",
        ") ",
        "SELECT ",
          "* EXCEPT(event_id, session_event_no, user_event_no, prev_event_name, seconds_from_prev_event, prev_hostname_norm, next_hostname_norm, is_cross_domain_hop, _dedup_rn), ",
          "TO_HEX(SHA256(CONCAT(user_pseudo_id, '|', CAST(event_timestamp_micros AS STRING), '|', event_name, '|', CAST(COALESCE(ga_session_id, -1) AS STRING)))) AS event_id, ",
          "ROW_NUMBER() OVER(PARTITION BY pseudonymous_session_id ORDER BY event_timestamp_micros, event_name) AS session_event_no, ",
          "ROW_NUMBER() OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp_micros, event_name) AS user_event_no, ",
          "LAG(event_name) OVER(PARTITION BY pseudonymous_session_id ORDER BY event_timestamp_micros, event_name) AS prev_event_name, ",
          "CAST((event_timestamp_micros - LAG(event_timestamp_micros) OVER(PARTITION BY pseudonymous_session_id ORDER BY event_timestamp_micros, event_name)) / 1000000 AS INT64) AS seconds_from_prev_event, ",
          "LAG(hostname_norm) OVER(PARTITION BY pseudonymous_session_id ORDER BY event_timestamp_micros, event_name) AS prev_hostname_norm, ",
          "LEAD(hostname_norm) OVER(PARTITION BY pseudonymous_session_id ORDER BY event_timestamp_micros, event_name) AS next_hostname_norm, ",
          "CASE WHEN LAG(hostname_norm) OVER(PARTITION BY pseudonymous_session_id ORDER BY event_timestamp_micros, event_name) IS NOT NULL ",
               "AND LAG(hostname_norm) OVER(PARTITION BY pseudonymous_session_id ORDER BY event_timestamp_micros, event_name) != hostname_norm ",
               "THEN TRUE ELSE FALSE END AS is_cross_domain_hop ",
        "FROM deduped WHERE _dedup_rn = 1"
      );
      EXECUTE IMMEDIATE sql_text;

      -- 13. Slotカラム一括UPDATE
      SET sql_text = CONCAT(
        "SELECT STRING_AGG(CONCAT('m.`', user_property_name, '` = u.`', user_property_name, '`'), ', ') ",
        "FROM `", full_target_path, ".mst_pseudonymous_properties`"
      );
      EXECUTE IMMEDIATE sql_text INTO p3_slot_update_set;

      IF p3_slot_update_set IS NOT NULL AND p3_slot_update_set != '' THEN
        IF p3_is_incremental THEN
          SET sql_text = CONCAT(
            "UPDATE `", full_target_path, ".micro_user_table` m SET ",
              p3_slot_update_set, ", ",
              "m.last_active_timestamp = u.last_active_timestamp, m.last_active_date = u.last_active_date, ",
              "m.first_touch_timestamp = u.first_touch_timestamp, m.first_touch_date = u.first_touch_date, ",
              "m.updated_at = u.updated_at, m.source_table_date = u.source_table_date ",
            "FROM `", full_target_path, ".log_pseudonymous_users` u ",
            "WHERE m.user_pseudo_id = u.user_pseudo_id ",
            "AND m.event_date BETWEEN DATE('", CAST(p3_start_date AS STRING), "') AND DATE('", CAST(p3_end_date AS STRING), "')"
          );
        ELSE
          SET sql_text = CONCAT(
            "UPDATE `", full_target_path, ".micro_user_table` m SET ",
              p3_slot_update_set, ", ",
              "m.last_active_timestamp = u.last_active_timestamp, m.last_active_date = u.last_active_date, ",
              "m.first_touch_timestamp = u.first_touch_timestamp, m.first_touch_date = u.first_touch_date, ",
              "m.updated_at = u.updated_at, m.source_table_date = u.source_table_date ",
            "FROM `", full_target_path, ".log_pseudonymous_users` u ",
            "WHERE m.user_pseudo_id = u.user_pseudo_id"
          );
        END IF;
        EXECUTE IMMEDIATE sql_text;
      END IF;

      -- 14. 完了確認
      SET sql_text = CONCAT("SELECT COUNT(*) FROM `", full_target_path, ".micro_user_table`");
      EXECUTE IMMEDIATE sql_text INTO p3_result_count;

      IF ARRAY_LENGTH(p3_failed_events) = 0 THEN
        SET phase_3a_status = 'SUCCESS';
      ELSEIF p3_success_count > 0 THEN
        SET phase_3a_status = CONCAT(
          'PARTIAL: failed=', ARRAY_TO_STRING(p3_failed_events, ','),
          ' | first_error=', IFNULL(p3_first_error_msg, 'unknown')
        );
      ELSE
        SET phase_3a_status = CONCAT(
          'FAILED: all event inserts failed. events=', ARRAY_TO_STRING(p3_failed_events, ','),
          ' | first_error=', IFNULL(p3_first_error_msg, 'unknown')
        );
      END IF;
    EXCEPTION WHEN ERROR THEN
      SET phase_3a_status = CONCAT('FAILED: ', @@error.message);
    END;
  ELSE
    SET phase_3a_status = 'SKIPPED';
  END IF;

  -- ==================================================================================
  -- 【方式】CONCAT + 変数q（Phase 2と同一方式）
  -- 【LEFT JOIN必須】dim_user_map_180d に対する JOIN は LEFT JOIN のみ
  -- ==================================================================================
  IF STARTS_WITH(phase_3a_status, 'SUCCESS') OR STARTS_WITH(phase_3a_status, 'PARTIAL') THEN
    BEGIN
      -- 変数初期化
      SET p3b_table_suffix_start = FORMAT_DATE('%Y%m%d', DATE_SUB(PARSE_DATE('%Y%m%d', start_date_str), INTERVAL 1 DAY));
      SET p3b_table_suffix_end = end_date_str;
    IF is_incremental THEN
        BEGIN
          EXECUTE IMMEDIATE CONCAT(
            'DELETE FROM `', full_target_path, '.micro_items_table` ',
            'WHERE event_date BETWEEN ',
              q, FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', start_date_str)), q,
              ' AND ',
              q, FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', end_date_str)), q
          );
        EXCEPTION WHEN ERROR THEN
          -- テーブルが空の場合やevent_dateが存在しない場合は無視
          SET p3b_event_count = 0;
        END;
      ELSE
        -- 全量再作成: テーブルを空にする（スキーマはPhase 0で作成済み）
        BEGIN
          EXECUTE IMMEDIATE CONCAT(
            'DELETE FROM `', full_target_path, '.micro_items_table` WHERE TRUE'
          );
        EXCEPTION WHEN ERROR THEN
          SET p3b_event_count = 0;
        END;
      END IF;

      -- Step 3B.1: 対象イベントの検出（items配列が存在するイベント）
      EXECUTE IMMEDIATE CONCAT(
        'CREATE OR REPLACE TEMP TABLE _p3b_item_events AS ',
        'SELECT DISTINCT event_name ',
        'FROM `', full_source_path, '.events_*` e, UNNEST(e.items) AS item ',
        'WHERE e._TABLE_SUFFIX BETWEEN ', q, p3b_table_suffix_start, q, ' AND ', q, p3b_table_suffix_end, q
      );
      SET p3b_event_count = (SELECT COUNT(*) FROM _p3b_item_events);

      -- Step 3B.3: item_paramsの動的カラム展開SQL生成（mst_item_paramsに基づく）
      SET p3b_item_params_sql = NULL;
      BEGIN
        EXECUTE IMMEDIATE CONCAT(
          'SELECT STRING_AGG( ',
            'CASE ',
              'WHEN data_type = ', q, 'STRING', q, ' THEN CONCAT( ',
                q, '(SELECT value.string_value FROM UNNEST(item.item_params) WHERE key = "', q, ', param_name, ', q, '" LIMIT 1) AS `', q, ', micro_column_name, ', q, '`', q,
              ') ',
              'WHEN data_type = ', q, 'FLOAT64', q, ' THEN CONCAT( ',
                q, '(SELECT value.double_value FROM UNNEST(item.item_params) WHERE key = "', q, ', param_name, ', q, '" LIMIT 1) AS `', q, ', micro_column_name, ', q, '`', q,
              ') ',
              'ELSE CONCAT( ',
                q, '(SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = "', q, ', param_name, ', q, '" LIMIT 1) AS `', q, ', micro_column_name, ', q, '`', q,
              ') ',
            'END, ',
            q, ', ', q, ' ORDER BY micro_column_name',
          ') ',
          'FROM ( ',
            'SELECT param_name, micro_column_name, ',
              'CASE ',
                'WHEN COUNTIF(data_type = ', q, 'STRING', q, ') > 0 THEN ', q, 'STRING', q, ' ',
                'WHEN COUNTIF(data_type = ', q, 'FLOAT64', q, ') > 0 THEN ', q, 'FLOAT64', q, ' ',
                'ELSE ', q, 'INT64', q, ' ',
              'END AS data_type ',
            'FROM `', full_target_path, '.mst_item_params` ',
            'GROUP BY param_name, micro_column_name ',
          ')'
        ) INTO p3b_item_params_sql;
      EXCEPTION WHEN ERROR THEN
        SET p3b_item_params_sql = NULL;
      END;
      SET p3b_column_list = 'event_date, event_timestamp, event_timestamp_micros, event_name, user_pseudo_id, user_id, is_identified_user, ga_session_id, pseudonymous_session_id, transaction_id, ecommerce_total_value, item_index, item_id, item_name, item_brand, item_variant, item_category, item_category2, item_category3, item_category4, item_category5, price, quantity, coupon, item_subtotal, affiliation, location_id, item_list_id, item_list_name, item_list_index, promotion_id, promotion_name, creative_name, creative_slot, event_id, item_event_no, device_category, mobile_brand_name, operating_system, browser, device_language, hostname, continent, sub_continent, country, region, city, analytics_storage, platform, is_key_event';

      -- item_paramsの動的カラム名を追加
      IF p3b_item_params_sql IS NOT NULL AND p3b_item_params_sql != '' THEN
        SET p3b_item_col_names = NULL;
        BEGIN
          EXECUTE IMMEDIATE CONCAT(
            'SELECT STRING_AGG(DISTINCT micro_column_name, ', q, ', ', q, ' ORDER BY micro_column_name) FROM `', full_target_path, '.mst_item_params`'
          ) INTO p3b_item_col_names;
        EXCEPTION WHEN ERROR THEN
          SET p3b_item_col_names = NULL;
        END;
        IF p3b_item_col_names IS NOT NULL AND p3b_item_col_names != '' THEN
          SET p3b_column_list = CONCAT(p3b_column_list, ', ', p3b_item_col_names);
        END IF;
      END IF;

      -- 最後に固定のページ・トラフィック・created_at・batch_id列を追加
      SET p3b_column_list = CONCAT(p3b_column_list, ', page_location, traffic_source, traffic_medium, traffic_campaign, created_at, batch_id');

      -- Step 3B.4-5: 各対象イベントに対してINSERT
      FOR item_ev IN (SELECT event_name FROM _p3b_item_events)
      DO
        -- INSERT用SQL構築
        -- items標準フィールド（サフィックスなし）
        SET p3b_select_sql = CONCAT(
          'PARSE_DATE(', q, '%Y%m%d', q, ', e.event_date) AS event_date, ',
          'DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') AS event_timestamp, ',
          'e.event_timestamp AS event_timestamp_micros, ',
          'e.event_name, ',
          'e.user_pseudo_id, ',
          'u.user_id, ',
          'CASE WHEN u.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_identified_user, ',
          '(SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ' LIMIT 1) AS ga_session_id, ',          'CONCAT(e.user_pseudo_id, ', q, '_', q, ', CAST((SELECT COALESCE(value.int_value, CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = ', q, 'ga_session_id', q, ' LIMIT 1) AS STRING)) AS pseudonymous_session_id, ',
          '(SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = ', q, 'transaction_id', q, ' LIMIT 1) AS transaction_id, ',
          'e.ecommerce.purchase_revenue AS ecommerce_total_value, ',
          'item_index, ',
          'item.item_id, item.item_name, item.item_brand, item.item_variant, ',
          'item.item_category, item.item_category2, item.item_category3, ',
          'item.item_category4, item.item_category5, ',
          'item.price, item.quantity, ',
          'item.coupon, item.price * COALESCE(item.quantity, 1) AS item_subtotal, ',          'item.affiliation, item.location_id, ',
          'item.item_list_id, item.item_list_name, item.item_list_index, ',
          'item.promotion_id, item.promotion_name, ',
          'item.creative_name, item.creative_slot, ',
          'CAST(NULL AS STRING) AS event_id, CAST(NULL AS INT64) AS item_event_no, ',
          'e.device.category AS device_category, e.device.mobile_brand_name, ',
          'e.device.operating_system, e.device.web_info.browser AS browser, e.device.language AS device_language, ',
          'e.device.web_info.hostname AS hostname, ',          'e.geo.continent, e.geo.sub_continent, e.geo.country, e.geo.region, e.geo.city, ',
          'e.privacy_info.analytics_storage, e.platform, ',
          'CASE WHEN e.event_name = ', q, 'purchase', q, ' THEN 1 WHEN STARTS_WITH(e.event_name, ', q, 'CV_', q, ') THEN 1 ELSE 0 END AS is_key_event'
        );

        -- item_params動的カラム（存在する場合のみ追加）
        IF p3b_item_params_sql IS NOT NULL AND p3b_item_params_sql != '' THEN
          SET p3b_select_sql = CONCAT(p3b_select_sql, ', ', p3b_item_params_sql);
        END IF;

        -- イベント属性 + created_at + batch_id
        SET p3b_select_sql = CONCAT(p3b_select_sql, ', ',
          '(SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = ', q, 'page_location', q, ' LIMIT 1) AS page_location, ',
          'e.traffic_source.source AS traffic_source, ',
          'e.traffic_source.medium AS traffic_medium, ',
          'e.traffic_source.name AS traffic_campaign, ',
          'DATETIME(CURRENT_TIMESTAMP(), ', q, batch_timezone, q, ') AS created_at, ',
          q, batch_id, q, ' AS batch_id'
        );
        SET p3b_create_sql = CONCAT(
          'INSERT INTO `', full_target_path, '.micro_items_table` (', p3b_column_list, ') ',
          'SELECT ', p3b_select_sql, ' ',
          'FROM `', full_source_path, '.events_*` e, ',
          'UNNEST(e.items) AS item WITH OFFSET AS item_index ',
          'LEFT JOIN `', full_target_path, '.dim_user_map_180d` u ',
            'ON e.user_pseudo_id = u.user_pseudo_id ',
          'WHERE e._TABLE_SUFFIX BETWEEN ', q, p3b_table_suffix_start, q, ' AND ', q, p3b_table_suffix_end, q, ' ',
            'AND e.event_name = ', q, item_ev.event_name, q, ' ',
            'AND ARRAY_LENGTH(e.items) > 0 ',
            'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') >= DATETIME(', q, FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', start_date_str)), ' 00:00:00', q, ') ',
            'AND DATETIME(TIMESTAMP_MICROS(e.event_timestamp), ', q, batch_timezone, q, ') <= DATETIME(', q, FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', end_date_str)), ' 23:59:59', q, ')'
        );        BEGIN
          EXECUTE IMMEDIATE p3b_create_sql;
          SET p3b_success_count = p3b_success_count + 1;
        EXCEPTION WHEN ERROR THEN
          SET p3b_failed_events = ARRAY_CONCAT(p3b_failed_events, [item_ev.event_name]);
          SET p3b_failed_errors = ARRAY_CONCAT(p3b_failed_errors,
            [CONCAT(item_ev.event_name, ': ', SUBSTR(@@error.message, 1, 500))]);
          IF p3b_first_error_msg IS NULL THEN
            SET p3b_first_error_msg = CONCAT(item_ev.event_name, ': ', SUBSTR(@@error.message, 1, 500));
          END IF;
        END;
      END FOR;      -- 全INSERT完了後にウィンドウ関数で一括計算し、p3b_column_listの順序に基づくカラム位置を保証
      EXECUTE IMMEDIATE CONCAT(
        'CREATE OR REPLACE TABLE `', full_target_path, '.micro_items_table` ',
        'PARTITION BY event_date ',
        'CLUSTER BY user_pseudo_id, item_id, event_name ',
        'AS SELECT ', p3b_column_list, ' ',
        'FROM ( ',
          'SELECT * EXCEPT(event_id, item_event_no), ',
            'TO_HEX(SHA256(CONCAT(user_pseudo_id, ', q, '|', q, ', CAST(event_timestamp_micros AS STRING), ', q, '|', q, ', event_name, ', q, '|', q, ', CAST(COALESCE(ga_session_id, -1) AS STRING)))) AS event_id, ',
            'ROW_NUMBER() OVER(PARTITION BY pseudonymous_session_id, item_id ORDER BY event_timestamp_micros, event_name, item_index) AS item_event_no ',
          'FROM `', full_target_path, '.micro_items_table` ',
        ')'
      );
        IF ARRAY_LENGTH(p3b_failed_events) = 0 THEN
        SET phase_3b_status = 'SUCCESS';
      ELSEIF p3b_success_count > 0 THEN
        SET phase_3b_status = CONCAT(
          'PARTIAL: failed=', ARRAY_TO_STRING(p3b_failed_events, ','),
          ' | first_error=', IFNULL(p3b_first_error_msg, 'unknown')
        );
      ELSE
        SET phase_3b_status = CONCAT(
          'FAILED: all events failed. events=', ARRAY_TO_STRING(p3b_failed_events, ','),
          ' | first_error=', IFNULL(p3b_first_error_msg, 'unknown')
        );
      END IF;
    EXCEPTION WHEN ERROR THEN
      SET phase_3b_status = CONCAT('FAILED: ', @@error.message);
    END;
  ELSE
    SET phase_3b_status = 'SKIPPED';
  END IF;

  -- ================================================================================
  -- バッチ実行ログ記録
  -- ================================================================================
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.log_batch_execution`
    (batch_id, executed_at, start_date, end_date,
     phase_0_status, phase_1_status, phase_2_status, phase_3a_status, phase_3b_status,
     is_force_reset, is_incremental, total_rows, execution_time_seconds,
     processor_version, client_id)
    VALUES (
      @p_batch_id,
      CURRENT_TIMESTAMP(),
      @p_start_date, @p_end_date,
      @p_phase_0, @p_phase_1, @p_phase_2, @p_phase_3a, @p_phase_3b,
      @p_force_reset, @p_incremental,
      (SELECT COUNT(*) FROM `%s.micro_user_table`),
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), @p_batch_start, SECOND),
      'public-v0.1.0', @p_client_id
    )
  """, full_target_path, full_target_path)
  USING
    batch_id AS p_batch_id,
    start_date_str AS p_start_date,
    end_date_str AS p_end_date,
    phase_0_status AS p_phase_0,
    phase_1_status AS p_phase_1,
    phase_2_status AS p_phase_2,
    phase_3a_status AS p_phase_3a,
    phase_3b_status AS p_phase_3b,
    is_force_reset AS p_force_reset,
    is_incremental AS p_incremental,
    batch_start_time AS p_batch_start,
    client_id AS p_client_id;

  EXECUTE IMMEDIATE FORMAT("""
    DELETE FROM `%s.log_batch_lock`
    WHERE lock_name = @p_lock_name AND batch_id = @p_batch_id
  """, full_target_path)
  USING batch_lock_name AS p_lock_name, batch_id AS p_batch_id;

  -- ================================================================================
  -- 完了サマリー
  -- ================================================================================
  SELECT
    'WACA core batch public-v0.1.0' AS processor,
    start_date_str AS start_date,
    end_date_str AS end_date,
    phase_0_status,
    phase_1_status,
    phase_2_status,
    phase_3a_status,
    phase_3b_status,
    CASE WHEN is_incremental THEN 'ACCUMULATIVE' ELSE 'FULL_REBUILD' END AS update_mode,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), batch_start_time, SECOND) AS total_seconds;

END;
