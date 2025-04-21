--Step 1: Create the customers Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `sample_customer_target_table`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);


--Step 2: Update Existing Active Records if There Are Changes
MERGE INTO  `sample_customer_target_table` target
USING 
  (SELECT DISTINCT
    *, 
    CASE 
      WHEN customer_id IS NULL OR email IS NULL OR name IS NULL THEN TRUE
      ELSE FALSE
    END AS is_quarantined,
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    True as is_active
  FROM `sample_raw_customer_source_table`) source
ON target.customer_id = source.customer_id AND target.is_active = true
WHEN MATCHED AND 
            (
             target.name != source.name OR
             target.email != source.email OR
             target.updated_at != source.updated_at) 
    THEN UPDATE SET 
        target.is_active = false,
        target.effective_end_date = current_timestamp();

--Step 3: Insert New or Updated Records
MERGE INTO  `sample_customer_target_table` target
USING 
  (SELECT DISTINCT
    *, 
    CASE 
      WHEN customer_id IS NULL OR email IS NULL OR name IS NULL THEN TRUE
      ELSE FALSE
    END AS is_quarantined,
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    True as is_active
  FROM `sample_raw_customer_source_table`) source
ON target.customer_id = source.customer_id AND target.is_active = true
WHEN NOT MATCHED THEN 
    INSERT (customer_id, name, email, updated_at, is_quarantined, effective_start_date, effective_end_date, is_active)
    VALUES (source.customer_id, source.name, source.email, source.updated_at, source.is_quarantined, source.effective_start_date, source.effective_end_date, source.is_active);
