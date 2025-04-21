CREATE OR REPLACE TABLE
  `gcp-actual-pb.test.test_partition`
PARTITION BY
  DATE(updated_at) AS (
  SELECT
    customer_id,
    name,
    email,
    DATETIME(TIMESTAMP_MILLIS(CAST(updated_at AS INT64))) AS updated_at,
    is_quarantined,
    effective_start_date,
    effective_end_date,
    is_active
  FROM
    `gcp-actual-pb.silver_dataset.customers`);