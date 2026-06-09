-- noqa: disable=all
-- Create an anonymous GA4 BigQuery export sample for WACA core installation tests.
--
-- Usage:
--   bq query --use_legacy_sql=false \
--     --parameter=project_id:STRING:your-gcp-project-id \
--     --parameter=dataset_id:STRING:waca_core_sample_ga4 \
--     --parameter=location:STRING:asia-northeast1 \
--     < samples/bigquery/create_anonymous_ga4_sample.sql
--
-- This dataset contains synthetic users, synthetic pages, and synthetic item
-- data only. It must not be replaced with real customer logs.

DECLARE project_id STRING DEFAULT @project_id;
DECLARE dataset_id STRING DEFAULT @dataset_id;
DECLARE location STRING DEFAULT @location;
DECLARE full_dataset STRING DEFAULT CONCAT(project_id, '.', dataset_id);

EXECUTE IMMEDIATE FORMAT(
  "CREATE SCHEMA IF NOT EXISTS `%s` OPTIONS(location='%s')",
  full_dataset,
  location
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.events_20260501` AS
SELECT * FROM UNNEST([
  STRUCT(
    '20260501' AS event_date,
    1777611600000000 AS event_timestamp,
    'session_start' AS event_name,
    'anon_user_001' AS user_pseudo_id,
    CAST(NULL AS STRING) AS user_id,
    [
      STRUCT('ga_session_id' AS key, STRUCT(CAST(NULL AS STRING) AS string_value, 10001 AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value) AS value),
      STRUCT('page_location' AS key, STRUCT('https://example.test/' AS string_value, CAST(NULL AS INT64) AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value) AS value),
      STRUCT('page_title' AS key, STRUCT('Example Home' AS string_value, CAST(NULL AS INT64) AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value) AS value),
      STRUCT('engagement_time_msec' AS key, STRUCT(CAST(NULL AS STRING) AS string_value, 1200 AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value) AS value)
    ] AS event_params,
    [
      STRUCT('customer_type' AS key, STRUCT('anonymous_sample' AS string_value, CAST(NULL AS INT64) AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value, 1777611600000000 AS set_timestamp_micros) AS value)
    ] AS user_properties,
    ARRAY<STRUCT<
      item_id STRING,
      item_name STRING,
      item_brand STRING,
      item_variant STRING,
      item_category STRING,
      item_category2 STRING,
      item_category3 STRING,
      item_category4 STRING,
      item_category5 STRING,
      price FLOAT64,
      quantity INT64,
      coupon STRING,
      affiliation STRING,
      location_id STRING,
      item_list_id STRING,
      item_list_name STRING,
      item_list_index STRING,
      promotion_id STRING,
      promotion_name STRING,
      creative_name STRING,
      creative_slot STRING,
      item_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>
    >>[] AS items,
    STRUCT('desktop' AS category, 'SampleBrand' AS mobile_brand_name, 'SampleModel' AS mobile_model_name, 'macOS' AS operating_system, '14' AS operating_system_version, 'Chrome' AS browser, 'en-us' AS language, STRUCT('Chrome' AS browser, 'example.test' AS hostname) AS web_info) AS device,
    STRUCT('Asia' AS continent, 'Eastern Asia' AS sub_continent, 'Japan' AS country, 'Tokyo' AS region, 'Chiyoda' AS city) AS geo,
    STRUCT('google' AS source, 'organic' AS medium, 'anonymous-sample' AS name) AS traffic_source,
    STRUCT('google' AS manual_source, 'organic' AS manual_medium, 'anonymous-sample' AS manual_campaign_name, CAST(NULL AS STRING) AS manual_term, CAST(NULL AS STRING) AS manual_content, CAST(NULL AS STRING) AS gclid) AS collected_traffic_source,
    STRUCT('Yes' AS analytics_storage) AS privacy_info,
    STRUCT(CAST(NULL AS STRING) AS id, CAST(NULL AS STRING) AS version) AS app_info,
    'WEB' AS platform,
    STRUCT(CAST(NULL AS FLOAT64) AS purchase_revenue) AS ecommerce
  ),
  STRUCT(
    '20260501',
    1777611660000000,
    'page_view',
    'anon_user_001',
    CAST(NULL AS STRING),
    [
      STRUCT('ga_session_id', STRUCT(CAST(NULL AS STRING), 10001, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64))),
      STRUCT('page_location', STRUCT('https://example.test/pricing', CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64))),
      STRUCT('page_title', STRUCT('Example Pricing', CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64))),
      STRUCT('engagement_time_msec', STRUCT(CAST(NULL AS STRING), 4500, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)))
    ],
    [STRUCT('customer_type', STRUCT('anonymous_sample', CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), 1777611660000000))],
    ARRAY<STRUCT<
      item_id STRING,
      item_name STRING,
      item_brand STRING,
      item_variant STRING,
      item_category STRING,
      item_category2 STRING,
      item_category3 STRING,
      item_category4 STRING,
      item_category5 STRING,
      price FLOAT64,
      quantity INT64,
      coupon STRING,
      affiliation STRING,
      location_id STRING,
      item_list_id STRING,
      item_list_name STRING,
      item_list_index STRING,
      promotion_id STRING,
      promotion_name STRING,
      creative_name STRING,
      creative_slot STRING,
      item_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>
    >>[],
    STRUCT('desktop', 'SampleBrand', 'SampleModel', 'macOS', '14', 'Chrome', 'en-us', STRUCT('Chrome', 'example.test')),
    STRUCT('Asia', 'Eastern Asia', 'Japan', 'Tokyo', 'Chiyoda'),
    STRUCT('google', 'organic', 'anonymous-sample'),
    STRUCT('google', 'organic', 'anonymous-sample', CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)),
    STRUCT('Yes'),
    STRUCT(CAST(NULL AS STRING), CAST(NULL AS STRING)),
    'WEB',
    STRUCT(CAST(NULL AS FLOAT64))
  ),
  STRUCT(
    '20260501',
    1777611900000000,
    'purchase',
    'anon_user_001',
    CAST(NULL AS STRING),
    [
      STRUCT('ga_session_id', STRUCT(CAST(NULL AS STRING), 10001, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64))),
      STRUCT('page_location', STRUCT('https://example.test/checkout/thank-you', CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64))),
      STRUCT('transaction_id', STRUCT('sample_tx_001', CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64))),
      STRUCT('engagement_time_msec', STRUCT(CAST(NULL AS STRING), 2600, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)))
    ],
    [STRUCT('customer_type', STRUCT('anonymous_sample', CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), 1777611900000000))],
    [
      STRUCT(
        'sample_item_001',
        'Sample Starter Plan',
        'WACA Sample',
        'monthly',
        'service',
        'starter',
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        1200.0,
        1,
        CAST(NULL AS STRING),
        'example.test',
        CAST(NULL AS STRING),
        'sample_list',
        'Sample List',
        '1',
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        [STRUCT('sample_item_type', STRUCT('subscription', CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)))]
      )
    ],
    STRUCT('desktop', 'SampleBrand', 'SampleModel', 'macOS', '14', 'Chrome', 'en-us', STRUCT('Chrome', 'example.test')),
    STRUCT('Asia', 'Eastern Asia', 'Japan', 'Tokyo', 'Chiyoda'),
    STRUCT('google', 'organic', 'anonymous-sample'),
    STRUCT('google', 'organic', 'anonymous-sample', CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)),
    STRUCT('Yes'),
    STRUCT(CAST(NULL AS STRING), CAST(NULL AS STRING)),
    'WEB',
    STRUCT(1200.0)
  )
])
""", full_dataset);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.events_20260502` AS
SELECT * FROM UNNEST([
  STRUCT(
    '20260502' AS event_date,
    1777698000000000 AS event_timestamp,
    'session_start' AS event_name,
    'anon_user_002' AS user_pseudo_id,
    CAST(NULL AS STRING) AS user_id,
    [
      STRUCT('ga_session_id' AS key, STRUCT(CAST(NULL AS STRING) AS string_value, 20001 AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value) AS value),
      STRUCT('page_location' AS key, STRUCT('https://example.test/articles/guide' AS string_value, CAST(NULL AS INT64) AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value) AS value),
      STRUCT('page_title' AS key, STRUCT('Example Guide' AS string_value, CAST(NULL AS INT64) AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value) AS value)
    ] AS event_params,
    [STRUCT('customer_type' AS key, STRUCT('anonymous_sample' AS string_value, CAST(NULL AS INT64) AS int_value, CAST(NULL AS FLOAT64) AS float_value, CAST(NULL AS FLOAT64) AS double_value, 1777698000000000 AS set_timestamp_micros) AS value)] AS user_properties,
    ARRAY<STRUCT<
      item_id STRING,
      item_name STRING,
      item_brand STRING,
      item_variant STRING,
      item_category STRING,
      item_category2 STRING,
      item_category3 STRING,
      item_category4 STRING,
      item_category5 STRING,
      price FLOAT64,
      quantity INT64,
      coupon STRING,
      affiliation STRING,
      location_id STRING,
      item_list_id STRING,
      item_list_name STRING,
      item_list_index STRING,
      promotion_id STRING,
      promotion_name STRING,
      creative_name STRING,
      creative_slot STRING,
      item_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>
    >>[] AS items,
    STRUCT('mobile' AS category, 'SamplePhone' AS mobile_brand_name, 'SampleModel' AS mobile_model_name, 'iOS' AS operating_system, '17' AS operating_system_version, 'Safari' AS browser, 'ja-jp' AS language, STRUCT('Safari' AS browser, 'example.test' AS hostname) AS web_info) AS device,
    STRUCT('Asia' AS continent, 'Eastern Asia' AS sub_continent, 'Japan' AS country, 'Osaka' AS region, 'Osaka' AS city) AS geo,
    STRUCT('newsletter' AS source, 'email' AS medium, 'anonymous-sample' AS name) AS traffic_source,
    STRUCT('newsletter' AS manual_source, 'email' AS manual_medium, 'anonymous-sample' AS manual_campaign_name, CAST(NULL AS STRING) AS manual_term, CAST(NULL AS STRING) AS manual_content, CAST(NULL AS STRING) AS gclid) AS collected_traffic_source,
    STRUCT('Yes' AS analytics_storage) AS privacy_info,
    STRUCT(CAST(NULL AS STRING) AS id, CAST(NULL AS STRING) AS version) AS app_info,
    'WEB' AS platform,
    STRUCT(CAST(NULL AS FLOAT64) AS purchase_revenue) AS ecommerce
  )
])
""", full_dataset);

SELECT
  full_dataset AS sample_dataset,
  'events_20260501 and events_20260502 created with anonymous synthetic data' AS status;
