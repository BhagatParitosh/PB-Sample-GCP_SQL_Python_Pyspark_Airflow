-- create storage integration

CREATE OR REPLACE STORAGE INTEGRATION GCP_STORAGE_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = GCS
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://avd-gcp-snow-bucket/click-stream');


DESC STORAGE INTEGRATION GCP_STORAGE_INT;

CREATE OR REPLACE STAGE SNOW_GCS_STAGE
    URL = 'gcs://avd-gcp-snow-bucket/click-stream'
    STORAGE_INTEGRATION = GCP_STORAGE_INT;


LIST @SNOW_GCS_STAGE;


-- create schema for file formats
CREATE OR REPLACE SCHEMA CLICK_STREAM.FILE_FORMATS;

-- create json file format
CREATE OR REPLACE FILE FORMAT CLICK_STREAM.FILE_FORMATS.JSON_FORMAT
    TYPE = JSON;
    
select $1 from @SNOW_GCS_STAGE
    (FILE_FORMAT => CLICK_STREAM.FILE_FORMATS.JSON_FORMAT);

--  create raw table for storing raw data and some metadata of the table

CREATE OR REPLACE TABLE RAW_CLICK_STREAM_DATA(
    RECORD_TS TIMESTAMP,
    JSON_DATA VARIANT,
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 STRING,
    _TABLE_LOAD_TS TIMESTAMP
);


-- create a task that runs for every 1 minute and loads the data from stage into raw table via copy command
CREATE OR REPLACE TASK RAW_COPY_DATA_TASK
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '1 MINUTE'
AS
    COPY INTO CLICK_STREAM.PUBLIC.RAW_CLICK_STREAM_DATA
    FROM (
        select 
            $1:timestamp::TIMESTAMP,
            $1::variant,
            metadata$filename as _STG_FILE_NAME,
            metadata$FILE_LAST_MODIFIED as _STG_FILE_LOAD_TS,
            metadata$FILE_CONTENT_KEY as _STG_FILE_MD5,
            current_timestamp() as _TABLE_LOAD_TS
        from @SNOW_GCS_STAGE
    )
FILE_FORMAT = CLICK_STREAM.FILE_FORMATS.JSON_FORMAT
ON_ERROR = ABORT_STATEMENT;

ALTER TASK RAW_COPY_DATA_TASK RESUME;

SELECT * FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'RAW_COPY_DATA_TASK'
    )
);



SELECT * FROM CLICK_STREAM.PUBLIC.RAW_CLICK_STREAM_DATA;


select
    JSON_DATA:user_agent::string AS USER_AGENT,
    JSON_DATA:device_type::string AS DEVICE_TYPE,
    JSON_DATA:element_class::string AS ELEMENT_CLASS,
    JSON_DATA:element_id::string AS ELEMENT_ID,
    JSON_DATA:element_type::string AS ELEMENT_TYPE,
    JSON_DATA:event_type::string AS EVENT_TYPE,
    JSON_DATA:language::string AS LANGUAGE,
    JSON_DATA:link::string AS LINK,
    JSON_DATA:page::string AS PAGE,
    JSON_DATA:referrer::string AS DOMAIN_NAME,
    JSON_DATA:screen_resolution::string AS SCREEN_RESOLUTION,
    JSON_DATA:location.ip::string AS LOCATION_IP,
    JSON_DATA:location.city::string AS LOCATION_CITY,
    JSON_DATA:location.country::string AS LOCATION_COUNTRY,
    JSON_DATA:location.region::string AS LOCATION_REGION,
    SPLIT_PART(JSON_DATA:location.loc, ',', 1)::string AS LOCATION_LATITUDE,
    SPLIT_PART(JSON_DATA:location.loc, ',', 2)::string AS LOCATION_LONGITUDE
FROM RAW_CLICK_STREAM_DATA;
    


SELECT 
    RECORD_TS,
    JSON_DATA,
    _STG_FILE_NAME ,
    _STG_FILE_LOAD_TS ,
    _STG_FILE_MD5,
    _TABLE_LOAD_TS, 
    ROW_NUMBER() OVER (PARTITION BY RECORD_TS ORDER BY _STG_FILE_LOAD_TS DESC) AS _LATEST_RANK
FROM RAW_CLICK_STREAM_DATA;




-- create a new table by removing the duplicates and applying the transformation
CREATE OR REPLACE DYNAMIC TABLE CLICK_STREAM.PUBLIC.CLEAN_CLICK_STREAM_DATA
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = 'COMPUTE_WH'
AS
    WITH _step1 AS (
        SELECT 
            RECORD_TS,
            JSON_DATA,
            _STG_FILE_NAME ,
            _STG_FILE_LOAD_TS ,
            _STG_FILE_MD5,
            _TABLE_LOAD_TS, 
            ROW_NUMBER() OVER (PARTITION BY RECORD_TS ORDER BY _STG_FILE_LOAD_TS DESC) AS _LATEST_RANK
        FROM RAW_CLICK_STREAM_DATA
    ),
    _step2 as (
        select
            JSON_DATA:user_agent::string AS USER_AGENT,
            JSON_DATA:device_type::string AS DEVICE_TYPE,
            JSON_DATA:element_class::string AS ELEMENT_CLASS,
            JSON_DATA:element_id::string AS ELEMENT_ID,
            JSON_DATA:element_type::string AS ELEMENT_TYPE,
            JSON_DATA:event_type::string AS EVENT_TYPE,
            JSON_DATA:language::string AS LANGUAGE,
            JSON_DATA:link::string AS LINK,
            JSON_DATA:page::string AS PAGE,
            JSON_DATA:referrer::string AS DOMAIN_NAME,
            JSON_DATA:screen_resolution::string AS SCREEN_RESOLUTION,
            JSON_DATA:location.ip::string AS LOCATION_IP,
            JSON_DATA:location.city::string AS LOCATION_CITY,
            JSON_DATA:location.country::string AS LOCATION_COUNTRY,
            JSON_DATA:location.region::string AS LOCATION_REGION,
            SPLIT_PART(JSON_DATA:location.loc, ',', 1)::string AS LOCATION_LATITUDE,
            SPLIT_PART(JSON_DATA:location.loc, ',', 2)::string AS LOCATION_LONGITUDE
        FROM _step1
        WHERE _LATEST_RANK = 1
    )
    select * from _step2;


select * from CLICK_STREAM.PUBLIC.CLEAN_CLICK_STREAM_DATA;



-- create a udf for parsing the user_agent values


CREATE OR REPLACE SCHEMA CLICK_STREAM.UDFS;

CREATE OR REPLACE FUNCTION CLICK_STREAM.UDFS.PARSE_USER_AGENT(user_agent string)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'parse_user_agent'
PACKAGES = ('user-agents')
AS
$$
from user_agents import parse

def parse_user_agent(user_agent):
    ua = parse(user_agent)
    return {
        "browser": ua.browser.family,
        "os": ua.os.family,
        "device": ua.device.family,
        "is_mobile": ua.is_mobile,
        "is_tablet": ua.is_tablet,
        "is_pc": ua.is_pc
    }
$$;

select CLICK_STREAM.UDFS.PARSE_USER_AGENT('Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1');



-- create a new dynamic table user_agent_dim that uses above udf to create new columns

CREATE OR REPLACE DYNAMIC TABLE CLICK_STREAM.PUBLIC.USER_AGENT_DIM
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = 'COMPUTE_WH'
AS
    WITH _step1 AS (
        SELECT DISTINCT USER_AGENT
        FROM CLICK_STREAM.PUBLIC.CLEAN_CLICK_STREAM_DATA
    ),
    _step2 AS (
        SELECT
            USER_AGENT,
            CLICK_STREAM.UDFS.PARSE_USER_AGENT(USER_AGENT) AS USER_AGENT_DETAILS
            FROM _step1
    )
    SELECT 
        USER_AGENT,
        USER_AGENT_DETAILS:browser::STRING AS BROWSER,
        USER_AGENT_DETAILS:os::STRING AS OS,
        USER_AGENT_DETAILS:device::STRING AS DEVICE,
        USER_AGENT_DETAILS:is_mobile::STRING AS IS_MOBILE,
        USER_AGENT_DETAILS:is_tablet::STRING AS IS_TABLET,
        USER_AGENT_DETAILS:is_pc::STRING AS IS_PC
    FROM _step2;
        


select * from CLICK_STREAM.PUBLIC.USER_AGENT_DIM;













    


