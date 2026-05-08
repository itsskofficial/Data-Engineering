USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.LOCATION_SRC (
    locationid TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    activeflag TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw location rows from CSV (all VARCHAR until refined).';

CREATE OR REPLACE STREAM LANDING.LOCATION_SRC_STREAM
    ON TABLE LANDING.LOCATION_SRC
    APPEND_ONLY = TRUE;

-- Initial bulk path (place CSV under @RAW_FILES_STG/initial/location/)
COPY INTO LANDING.LOCATION_SRC (
    locationid, city, state, zipcode, activeflag, createddate, modifieddate,
    _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1::TEXT, $2::TEXT, $3::TEXT, $4::TEXT, $5::TEXT, $6::TEXT, $7::TEXT,
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/location/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.DIM_LOCATION_HUB (
    location_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    location_id NUMBER NOT NULL UNIQUE,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    state_iso CHAR(2) NOT NULL,
    is_union_territory BOOLEAN NOT NULL DEFAULT FALSE,
    is_state_capital_city BOOLEAN NOT NULL DEFAULT FALSE,
    market_tier VARCHAR(20),
    postal_code VARCHAR(12) NOT NULL,
    is_active VARCHAR(10) NOT NULL,
    row_effective_ts TIMESTAMP_TZ NOT NULL,
    row_updated_ts TIMESTAMP_TZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.DIM_LOCATION_HUB_STREAM
    ON TABLE REFINED.DIM_LOCATION_HUB;

MERGE INTO REFINED.DIM_LOCATION_HUB AS tgt
USING (
    WITH state_iso_map (raw_state, iso) AS (
        SELECT * FROM VALUES
            ('Delhi', 'DL'), ('Maharashtra', 'MH'), ('Uttar Pradesh', 'UP'),
            ('Gujarat', 'GJ'), ('Rajasthan', 'RJ'), ('Kerala', 'KL'), ('Punjab', 'PB'),
            ('Karnataka', 'KA'), ('Madhya Pradesh', 'MP'), ('Odisha', 'OR'),
            ('Chandigarh', 'CH'), ('West Bengal', 'WB'), ('Sikkim', 'SK'),
            ('Andhra Pradesh', 'AP'), ('Assam', 'AS'), ('Jammu and Kashmir', 'JK'),
            ('Puducherry', 'PY'), ('Uttarakhand', 'UK'), ('Himachal Pradesh', 'HP'),
            ('Tamil Nadu', 'TN'), ('Goa', 'GA'), ('Telangana', 'TG'),
            ('Chhattisgarh', 'CG'), ('Jharkhand', 'JH'), ('Bihar', 'BR')
        AS s(raw_state, iso)
    )
    SELECT
        TRY_TO_NUMBER(l.locationid) AS location_id,
        TRIM(l.city)::VARCHAR AS city,
        IFF(TRIM(l.state) = 'Delhi', 'New Delhi', TRIM(l.state))::VARCHAR AS state,
        COALESCE(m.iso, 'UN') AS state_iso,
        IFF(
            TRIM(l.state) IN ('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir'),
            TRUE,
            FALSE
        ) AS is_union_territory,
        IFF(
            (TRIM(l.state) = 'Delhi' AND TRIM(l.city) = 'New Delhi')
            OR (TRIM(l.state) = 'Maharashtra' AND TRIM(l.city) = 'Mumbai'),
            TRUE,
            FALSE
        ) AS is_state_capital_city,
        CASE
            WHEN TRIM(l.city) IN (
                'Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad'
            ) THEN 'TIER_1'
            WHEN TRIM(l.city) IN (
                'Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara',
                'Coimbatore', 'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur',
                'Guwahati', 'Chandigarh'
            ) THEN 'TIER_2'
            ELSE 'TIER_3_OR_BELOW'
        END AS market_tier,
        TRIM(l.zipcode)::VARCHAR AS postal_code,
        TRIM(l.activeflag)::VARCHAR AS is_active,
        TO_TIMESTAMP_TZ(l.createddate, 'YYYY-MM-DD HH24:MI:SS') AS row_effective_ts,
        TO_TIMESTAMP_TZ(l.modifieddate, 'YYYY-MM-DD HH24:MI:SS') AS row_updated_ts,
        l._src_file,
        l._src_file_ts,
        l._src_file_key,
        CURRENT_TIMESTAMP() AS _refined_at
    FROM LANDING.LOCATION_SRC_STREAM l
    LEFT JOIN state_iso_map m ON m.raw_state = TRIM(l.state)
) AS src
ON tgt.location_id = src.location_id
WHEN MATCHED AND (
    tgt.city != src.city OR tgt.state != src.state OR tgt.state_iso != src.state_iso
    OR tgt.is_union_territory != src.is_union_territory
    OR tgt.is_state_capital_city != src.is_state_capital_city
    OR tgt.market_tier != src.market_tier OR tgt.postal_code != src.postal_code
    OR tgt.is_active != src.is_active OR NVL(tgt.row_updated_ts, '1970-01-01') != NVL(src.row_updated_ts, '1970-01-01')
) THEN
    UPDATE SET
        city = src.city,
        state = src.state,
        state_iso = src.state_iso,
        is_union_territory = src.is_union_territory,
        is_state_capital_city = src.is_state_capital_city,
        market_tier = src.market_tier,
        postal_code = src.postal_code,
        is_active = src.is_active,
        row_updated_ts = src.row_updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = src._refined_at
WHEN NOT MATCHED THEN
    INSERT (
        location_id, city, state, state_iso, is_union_territory, is_state_capital_city,
        market_tier, postal_code, is_active, row_effective_ts, row_updated_ts,
        _src_file, _src_file_ts, _src_file_key, _refined_at
    )
    VALUES (
        src.location_id, src.city, src.state, src.state_iso, src.is_union_territory,
        src.is_state_capital_city, src.market_tier, src.postal_code, src.is_active,
        src.row_effective_ts, src.row_updated_ts, src._src_file, src._src_file_ts,
        src._src_file_key, src._refined_at
    );

USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_LOCATION (
    location_hk NUMBER PRIMARY KEY,
    location_id NUMBER NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    state_iso CHAR(2) NOT NULL,
    is_union_territory BOOLEAN NOT NULL DEFAULT FALSE,
    is_state_capital_city BOOLEAN NOT NULL DEFAULT FALSE,
    market_tier VARCHAR(20),
    postal_code VARCHAR(12) NOT NULL,
    is_active VARCHAR(10) NOT NULL,
    eff_start_ts TIMESTAMP_TZ NOT NULL,
    eff_end_ts TIMESTAMP_TZ,
    is_current_row BOOLEAN NOT NULL DEFAULT TRUE
)
COMMENT = 'SCD2 location dimension keyed by hash(location_id + geo attributes).';

MERGE INTO ANALYTICS.DIM_LOCATION AS d
USING REFINED.DIM_LOCATION_HUB_STREAM AS s
ON d.location_id = s.location_id AND d.is_active = s.is_active
WHEN MATCHED
    AND s.METADATA$ACTION = 'DELETE'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET
        eff_end_ts = CURRENT_TIMESTAMP(),
        is_current_row = FALSE
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        location_hk, location_id, city, state, state_iso, is_union_territory,
        is_state_capital_city, market_tier, postal_code, is_active,
        eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(s.city, s.state, s.state_iso, s.postal_code))),
        s.location_id, s.city, s.state, s.state_iso, s.is_union_territory,
        s.is_state_capital_city, s.market_tier, s.postal_code, s.is_active,
        CURRENT_TIMESTAMP(), NULL, TRUE
    )
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        location_hk, location_id, city, state, state_iso, is_union_territory,
        is_state_capital_city, market_tier, postal_code, is_active,
        eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(s.city, s.state, s.state_iso, s.postal_code))),
        s.location_id, s.city, s.state, s.state_iso, s.is_union_territory,
        s.is_state_capital_city, s.market_tier, s.postal_code, s.is_active,
        CURRENT_TIMESTAMP(), NULL, TRUE
    );

-- Optional: delta example (same pattern as tutorial)
-- COPY INTO ... FROM @LANDING.RAW_FILES_STG/delta/location/...
