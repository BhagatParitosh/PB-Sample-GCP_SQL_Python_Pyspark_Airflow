CREATE OR REPLACE TABLE
  `partitioned_table`
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
    `non_partitioned_table`);
