USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.DELIVERY_AGENT_SRC (
    deliveryagentid TEXT,
    name TEXT,
    phone TEXT,
    vehicletype TEXT,
    locationid TEXT,
    status TEXT,
    gender TEXT,
    rating TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.DELIVERY_AGENT_SRC_STREAM
    ON TABLE LANDING.DELIVERY_AGENT_SRC
    APPEND_ONLY = TRUE;

COPY INTO LANDING.DELIVERY_AGENT_SRC (
    deliveryagentid, name, phone, vehicletype, locationid, status, gender, rating,
    createddate, modifieddate, _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY, CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/delivery-agent/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.DIM_DELIVERY_AGENT_HUB (
    rider_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    rider_id NUMBER NOT NULL UNIQUE,
    rider_name VARCHAR(120) NOT NULL,
    rider_phone VARCHAR(20) NOT NULL,
    vehicle_class VARCHAR(40) NOT NULL,
    home_location_id NUMBER,
    rider_state VARCHAR(40),
    gender_code VARCHAR(10),
    avg_rating DECIMAL(4, 2),
    created_ts TIMESTAMP_NTZ,
    updated_ts TIMESTAMP_NTZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.DIM_DELIVERY_AGENT_HUB_STREAM
    ON TABLE REFINED.DIM_DELIVERY_AGENT_HUB;

MERGE INTO REFINED.DIM_DELIVERY_AGENT_HUB AS tgt
USING (
    SELECT
        TRY_TO_NUMBER(s.deliveryagentid) AS rider_id,
        TRIM(s.name) AS rider_name,
        TRIM(s.phone) AS rider_phone,
        TRIM(s.vehicletype) AS vehicle_class,
        TRY_TO_NUMBER(s.locationid) AS home_location_id,
        TRIM(s.status) AS rider_state,
        TRIM(s.gender) AS gender_code,
        TRY_TO_DECIMAL(s.rating, 4, 2) AS avg_rating,
        TRY_TO_TIMESTAMP_NTZ(s.createddate) AS created_ts,
        TRY_TO_TIMESTAMP_NTZ(s.modifieddate) AS updated_ts,
        s._src_file, s._src_file_ts, s._src_file_key
    FROM LANDING.DELIVERY_AGENT_SRC_STREAM s
) AS src
ON tgt.rider_id = src.rider_id
WHEN MATCHED THEN
    UPDATE SET
        rider_name = src.rider_name,
        rider_phone = src.rider_phone,
        vehicle_class = src.vehicle_class,
        home_location_id = src.home_location_id,
        rider_state = src.rider_state,
        gender_code = src.gender_code,
        avg_rating = src.avg_rating,
        created_ts = src.created_ts,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        rider_id, rider_name, rider_phone, vehicle_class, home_location_id, rider_state,
        gender_code, avg_rating, created_ts, updated_ts, _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.rider_id, src.rider_name, src.rider_phone, src.vehicle_class, src.home_location_id,
        src.rider_state, src.gender_code, src.avg_rating, src.created_ts, src.updated_ts,
        src._src_file, src._src_file_ts, src._src_file_key
    );

USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_DELIVERY_AGENT (
    delivery_agent_hk NUMBER PRIMARY KEY,
    delivery_agent_id NUMBER NOT NULL,
    rider_name VARCHAR(120) NOT NULL,
    rider_phone VARCHAR(20),
    vehicle_class VARCHAR(40),
    location_id_fk NUMBER,
    rider_state VARCHAR(40),
    gender_code VARCHAR(10),
    avg_rating DECIMAL(4, 2),
    eff_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    eff_end_ts TIMESTAMP_NTZ,
    is_current_row BOOLEAN DEFAULT TRUE
);

MERGE INTO ANALYTICS.DIM_DELIVERY_AGENT AS d
USING REFINED.DIM_DELIVERY_AGENT_HUB_STREAM AS s
ON d.delivery_agent_id = s.rider_id
    AND NVL(d.rider_name, '') = NVL(s.rider_name, '')
    AND NVL(d.rider_phone, '') = NVL(s.rider_phone, '')
    AND NVL(d.vehicle_class, '') = NVL(s.vehicle_class, '')
    AND NVL(d.location_id_fk, -1) = NVL(s.home_location_id, -1)
    AND NVL(d.rider_state, '') = NVL(s.rider_state, '')
    AND NVL(d.gender_code, '') = NVL(s.gender_code, '')
    AND NVL(d.avg_rating, 0) = NVL(s.avg_rating, 0)
WHEN MATCHED
    AND s.METADATA$ACTION = 'DELETE'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET eff_end_ts = CURRENT_TIMESTAMP(), is_current_row = FALSE
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        delivery_agent_hk, delivery_agent_id, rider_name, rider_phone, vehicle_class,
        location_id_fk, rider_state, gender_code, avg_rating, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.rider_id, s.rider_name, s.rider_phone, s.vehicle_class, s.home_location_id,
            s.rider_state, s.gender_code, s.avg_rating
        ))),
        s.rider_id, s.rider_name, s.rider_phone, s.vehicle_class, s.home_location_id,
        s.rider_state, s.gender_code, s.avg_rating, CURRENT_TIMESTAMP(), NULL, TRUE
    )
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        delivery_agent_hk, delivery_agent_id, rider_name, rider_phone, vehicle_class,
        location_id_fk, rider_state, gender_code, avg_rating, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.rider_id, s.rider_name, s.rider_phone, s.vehicle_class, s.home_location_id,
            s.rider_state, s.gender_code, s.avg_rating
        ))),
        s.rider_id, s.rider_name, s.rider_phone, s.vehicle_class, s.home_location_id,
        s.rider_state, s.gender_code, s.avg_rating, CURRENT_TIMESTAMP(), NULL, TRUE
    );
