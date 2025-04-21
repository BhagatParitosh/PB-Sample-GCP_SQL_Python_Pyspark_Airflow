-- Creating Snowpipe to load raw data continuously into raw table

// Step 1) 
-- Create a notification channel on gcp using the following command
-- $ gsutil notification create -t <topic> -f json -e OBJECT_FINALIZE gs://<bucket-name>

// Step 2)
-- Create a notification integration on snowflake
CREATE OR REPLACE NOTIFICATION INTEGRATION GCP_SNOWFLAKE_SNOWPIPE_INTEGRATION
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    ENABLED = TRUE
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/gcp-actual-pb/subscriptions/gcp-snowflake-snowpipe-subscription';

DESC NOTIFICATION INTEGRATION GCP_SNOWFLAKE_SNOWPIPE_INTEGRATION;

//Step 3) Follow the pubsub and permissions related steps mentioned in the snowflake documentation

// Step 4)
-- Create Snowpipe from gcs bucket on the TEST_RAW_DATA_TABLE
CREATE OR REPLACE PIPE GCS_TO_SNOWFLAKE_PIPE
    AUTO_INGEST = TRUE
    INTEGRATION = 'GCP_SNOWFLAKE_SNOWPIPE_INTEGRATION'
    AS
        COPY INTO CS_TEST.PUBLIC.TEST_RAW_DATA_TABLE
        FROM @GCP_EXT_STAGE
            FILE_FORMAT = CS_TEST.FILE_FORMATS.JSON_FORMAT;

-- Check the pipe status
SELECT SYSTEM$PIPE_STATUS('GCS_TO_SNOWFLAKE_PIPE');

-- Check contents in the table
SELECT * FROM CS_TEST.PUBLIC.TEST_RAW_DATA_TABLE;

-- Refresh pipe (only use the first time when there is no data in the raw table)
-- ALTER PIPE GCS_TO_SNOWFLAKE_PIPE REFRESH;
-- Pause pipe
ALTER PIPE GCS_TO_SNOWFLAKE_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;


-- To see the copy command logs on a table
SELECT * FROM TABLE
(
    INFORMATION_SCHEMA.COPY_HISTORY
    (
        TABLE_NAME => 'TEST_RAW_DATA_TABLE',
        START_TIME => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
    )
);


-- Change the default timezone to India time
-- ALTER ACCOUNT SET TIMEZONE = 'Asia/Kolkata';
