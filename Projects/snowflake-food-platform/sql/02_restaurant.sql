USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.RESTAURANT_SRC (
    restaurantid TEXT,
    name TEXT,
    cuisinetype TEXT,
    pricing_for_2 TEXT,
    restaurant_phone TEXT
        WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'SENSITIVE'),
    operatinghours TEXT,
    locationid TEXT,
    activeflag TEXT,
    openstatus TEXT,
    locality TEXT,
    restaurant_address TEXT,
    latitude TEXT,
    longitude TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.RESTAURANT_SRC_STREAM
    ON TABLE LANDING.RESTAURANT_SRC
    APPEND_ONLY = TRUE;

COPY INTO LANDING.RESTAURANT_SRC (
    restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone,
    operatinghours, locationid, activeflag, openstatus, locality,
    restaurant_address, latitude, longitude, createddate, modifieddate,
    _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/restaurant/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.DIM_RESTAURANT_HUB (
    restaurant_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    restaurant_id NUMBER NOT NULL UNIQUE,
    brand_name VARCHAR(120) NOT NULL,
    cuisine_family VARCHAR(80),
    price_for_two_inr NUMBER(12, 2),
    contact_phone VARCHAR(20)
        WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'SENSITIVE'),
    hours_text VARCHAR(200),
    location_id_fk NUMBER,
    is_active VARCHAR(10),
    open_state VARCHAR(20),
    neighborhood VARCHAR(120),
    full_address VARCHAR(500),
    lat DECIMAL(10, 6),
    lon DECIMAL(10, 6),
    created_ts TIMESTAMP_TZ,
    updated_ts TIMESTAMP_TZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.DIM_RESTAURANT_HUB_STREAM
    ON TABLE REFINED.DIM_RESTAURANT_HUB;

MERGE INTO REFINED.DIM_RESTAURANT_HUB AS tgt
USING (
    SELECT
        TRY_TO_NUMBER(r.restaurantid) AS restaurant_id,
        TRY_TO_VARCHAR(r.name) AS brand_name,
        TRY_TO_VARCHAR(r.cuisinetype) AS cuisine_family,
        TRY_TO_DECIMAL(r.pricing_for_2, 12, 2) AS price_for_two_inr,
        TRY_TO_VARCHAR(r.restaurant_phone) AS contact_phone,
        TRY_TO_VARCHAR(r.operatinghours) AS hours_text,
        TRY_TO_NUMBER(r.locationid) AS location_id_fk,
        TRY_TO_VARCHAR(r.activeflag) AS is_active,
        TRY_TO_VARCHAR(r.openstatus) AS open_state,
        TRY_TO_VARCHAR(r.locality) AS neighborhood,
        TRY_TO_VARCHAR(r.restaurant_address) AS full_address,
        TRY_TO_DECIMAL(r.latitude, 10, 6) AS lat,
        TRY_TO_DECIMAL(r.longitude, 10, 6) AS lon,
        TRY_TO_TIMESTAMP_TZ(r.createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_ts,
        TRY_TO_TIMESTAMP_TZ(r.modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS updated_ts,
        r._src_file, r._src_file_ts, r._src_file_key
    FROM LANDING.RESTAURANT_SRC_STREAM r
) AS src
ON tgt.restaurant_id = src.restaurant_id
WHEN MATCHED THEN
    UPDATE SET
        brand_name = src.brand_name,
        cuisine_family = src.cuisine_family,
        price_for_two_inr = src.price_for_two_inr,
        contact_phone = src.contact_phone,
        hours_text = src.hours_text,
        location_id_fk = src.location_id_fk,
        is_active = src.is_active,
        open_state = src.open_state,
        neighborhood = src.neighborhood,
        full_address = src.full_address,
        lat = src.lat,
        lon = src.lon,
        created_ts = src.created_ts,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        restaurant_id, brand_name, cuisine_family, price_for_two_inr, contact_phone,
        hours_text, location_id_fk, is_active, open_state, neighborhood, full_address,
        lat, lon, created_ts, updated_ts, _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.restaurant_id, src.brand_name, src.cuisine_family, src.price_for_two_inr,
        src.contact_phone, src.hours_text, src.location_id_fk, src.is_active,
        src.open_state, src.neighborhood, src.full_address, src.lat, src.lon,
        src.created_ts, src.updated_ts, src._src_file, src._src_file_ts, src._src_file_key
    );

USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_RESTAURANT (
    restaurant_hk NUMBER PRIMARY KEY,
    restaurant_id NUMBER NOT NULL,
    brand_name VARCHAR(120) NOT NULL,
    cuisine_family VARCHAR(80),
    price_for_two_inr NUMBER(12, 2),
    contact_phone VARCHAR(20) WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'SENSITIVE'),
    hours_text VARCHAR(200),
    location_id_fk NUMBER,
    is_active VARCHAR(10),
    open_state VARCHAR(20),
    neighborhood VARCHAR(120),
    full_address VARCHAR(500),
    lat DECIMAL(10, 6),
    lon DECIMAL(10, 6),
    eff_start_ts TIMESTAMP_TZ,
    eff_end_ts TIMESTAMP_TZ,
    is_current_row BOOLEAN
);

MERGE INTO ANALYTICS.DIM_RESTAURANT AS d
USING REFINED.DIM_RESTAURANT_HUB_STREAM AS s
ON d.restaurant_id = s.restaurant_id
    AND d.brand_name = s.brand_name
    AND NVL(d.cuisine_family, '') = NVL(s.cuisine_family, '')
    AND NVL(d.price_for_two_inr, 0) = NVL(s.price_for_two_inr, 0)
    AND NVL(d.contact_phone, '') = NVL(s.contact_phone, '')
    AND NVL(d.hours_text, '') = NVL(s.hours_text, '')
    AND NVL(d.location_id_fk, -1) = NVL(s.location_id_fk, -1)
    AND NVL(d.is_active, '') = NVL(s.is_active, '')
    AND NVL(d.open_state, '') = NVL(s.open_state, '')
    AND NVL(d.neighborhood, '') = NVL(s.neighborhood, '')
    AND NVL(d.full_address, '') = NVL(s.full_address, '')
    AND NVL(d.lat, 0) = NVL(s.lat, 0)
    AND NVL(d.lon, 0) = NVL(s.lon, 0)
WHEN MATCHED
    AND s.METADATA$ACTION = 'DELETE'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET eff_end_ts = CURRENT_TIMESTAMP(), is_current_row = FALSE
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        restaurant_hk, restaurant_id, brand_name, cuisine_family, price_for_two_inr,
        contact_phone, hours_text, location_id_fk, is_active, open_state, neighborhood,
        full_address, lat, lon, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.restaurant_id, s.brand_name, s.cuisine_family, s.price_for_two_inr,
            s.contact_phone, s.hours_text, s.location_id_fk, s.is_active, s.open_state,
            s.neighborhood, s.full_address, s.lat, s.lon
        ))),
        s.restaurant_id, s.brand_name, s.cuisine_family, s.price_for_two_inr,
        s.contact_phone, s.hours_text, s.location_id_fk, s.is_active, s.open_state,
        s.neighborhood, s.full_address, s.lat, s.lon,
        CURRENT_TIMESTAMP(), NULL, TRUE
    )
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        restaurant_hk, restaurant_id, brand_name, cuisine_family, price_for_two_inr,
        contact_phone, hours_text, location_id_fk, is_active, open_state, neighborhood,
        full_address, lat, lon, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.restaurant_id, s.brand_name, s.cuisine_family, s.price_for_two_inr,
            s.contact_phone, s.hours_text, s.location_id_fk, s.is_active, s.open_state,
            s.neighborhood, s.full_address, s.lat, s.lon
        ))),
        s.restaurant_id, s.brand_name, s.cuisine_family, s.price_for_two_inr,
        s.contact_phone, s.hours_text, s.location_id_fk, s.is_active, s.open_state,
        s.neighborhood, s.full_address, s.lat, s.lon,
        CURRENT_TIMESTAMP(), NULL, TRUE
    );

ALTER TABLE ANALYTICS.DIM_RESTAURANT ALTER COLUMN contact_phone SET MASKING POLICY GOVERNANCE.MASK_PHONE;
