USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.CUSTOMER_ADDRESS_SRC (
    addressid TEXT,
    customerid TEXT,
    flatno TEXT,
    houseno TEXT,
    floor TEXT,
    building TEXT,
    landmark TEXT,
    locality TEXT,
    city TEXT,
    state TEXT,
    pincode TEXT,
    coordinates TEXT,
    primaryflag TEXT,
    addresstype TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.CUSTOMER_ADDRESS_SRC_STREAM
    ON TABLE LANDING.CUSTOMER_ADDRESS_SRC
    APPEND_ONLY = TRUE;

COPY INTO LANDING.CUSTOMER_ADDRESS_SRC (
    addressid, customerid, flatno, houseno, floor, building, landmark, locality,
    city, state, pincode, coordinates, primaryflag, addresstype, createddate, modifieddate,
    _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY, CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/customer-address/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.DIM_CUSTOMER_ADDRESS_HUB (
    address_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    address_id NUMBER NOT NULL UNIQUE,
    customer_uid_fk VARCHAR(40) NOT NULL,
    flat_no VARCHAR(40),
    house_no VARCHAR(40),
    floor_label VARCHAR(40),
    tower_block VARCHAR(120),
    near_landmark VARCHAR(200),
    area_locality VARCHAR(120),
    city_name VARCHAR(100),
    state_name VARCHAR(100),
    postal_code VARCHAR(12),
    geo_coords VARCHAR(80),
    is_primary VARCHAR(5),
    address_kind VARCHAR(40),
    created_ts TIMESTAMP_TZ,
    updated_ts TIMESTAMP_TZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.DIM_CUSTOMER_ADDRESS_HUB_STREAM
    ON TABLE REFINED.DIM_CUSTOMER_ADDRESS_HUB;

MERGE INTO REFINED.DIM_CUSTOMER_ADDRESS_HUB AS tgt
USING (
    SELECT
        TRY_TO_NUMBER(a.addressid) AS address_id,
        TRIM(a.customerid) AS customer_uid_fk,
        a.flatno AS flat_no,
        a.houseno AS house_no,
        a.floor AS floor_label,
        a.building AS tower_block,
        a.landmark AS near_landmark,
        a.locality AS area_locality,
        a.city AS city_name,
        a.state AS state_name,
        a.pincode AS postal_code,
        a.coordinates AS geo_coords,
        a.primaryflag AS is_primary,
        a.addresstype AS address_kind,
        TRY_TO_TIMESTAMP_TZ(a.createddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_ts,
        TRY_TO_TIMESTAMP_TZ(a.modifieddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_ts,
        a._src_file, a._src_file_ts, a._src_file_key
    FROM LANDING.CUSTOMER_ADDRESS_SRC_STREAM a
) AS src
ON tgt.address_id = src.address_id
WHEN MATCHED THEN
    UPDATE SET
        customer_uid_fk = src.customer_uid_fk,
        flat_no = src.flat_no,
        house_no = src.house_no,
        floor_label = src.floor_label,
        tower_block = src.tower_block,
        near_landmark = src.near_landmark,
        area_locality = src.area_locality,
        city_name = src.city_name,
        state_name = src.state_name,
        postal_code = src.postal_code,
        geo_coords = src.geo_coords,
        is_primary = src.is_primary,
        address_kind = src.address_kind,
        created_ts = src.created_ts,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        address_id, customer_uid_fk, flat_no, house_no, floor_label, tower_block,
        near_landmark, area_locality, city_name, state_name, postal_code, geo_coords,
        is_primary, address_kind, created_ts, updated_ts, _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.address_id, src.customer_uid_fk, src.flat_no, src.house_no, src.floor_label,
        src.tower_block, src.near_landmark, src.area_locality, src.city_name, src.state_name,
        src.postal_code, src.geo_coords, src.is_primary, src.address_kind, src.created_ts,
        src.updated_ts, src._src_file, src._src_file_ts, src._src_file_key
    );

USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_CUSTOMER_ADDRESS (
    address_hk NUMBER PRIMARY KEY,
    address_id NUMBER NOT NULL,
    customer_uid_fk VARCHAR(40) NOT NULL,
    flat_no VARCHAR(40),
    house_no VARCHAR(40),
    floor_label VARCHAR(40),
    tower_block VARCHAR(120),
    near_landmark VARCHAR(200),
    area_locality VARCHAR(120),
    city_name VARCHAR(100),
    state_name VARCHAR(100),
    postal_code VARCHAR(12),
    geo_coords VARCHAR(80),
    is_primary VARCHAR(5),
    address_kind VARCHAR(40),
    eff_start_ts TIMESTAMP_TZ,
    eff_end_ts TIMESTAMP_TZ,
    is_current_row BOOLEAN
);

MERGE INTO ANALYTICS.DIM_CUSTOMER_ADDRESS AS d
USING REFINED.DIM_CUSTOMER_ADDRESS_HUB_STREAM AS s
ON d.address_id = s.address_id
    AND NVL(d.customer_uid_fk, '') = NVL(s.customer_uid_fk, '')
    AND NVL(d.flat_no, '') = NVL(s.flat_no, '')
    AND NVL(d.house_no, '') = NVL(s.house_no, '')
    AND NVL(d.floor_label, '') = NVL(s.floor_label, '')
    AND NVL(d.tower_block, '') = NVL(s.tower_block, '')
    AND NVL(d.near_landmark, '') = NVL(s.near_landmark, '')
    AND NVL(d.area_locality, '') = NVL(s.area_locality, '')
    AND NVL(d.city_name, '') = NVL(s.city_name, '')
    AND NVL(d.state_name, '') = NVL(s.state_name, '')
    AND NVL(d.postal_code, '') = NVL(s.postal_code, '')
    AND NVL(d.geo_coords, '') = NVL(s.geo_coords, '')
    AND NVL(d.is_primary, '') = NVL(s.is_primary, '')
    AND NVL(d.address_kind, '') = NVL(s.address_kind, '')
WHEN MATCHED
    AND s.METADATA$ACTION = 'DELETE'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET eff_end_ts = CURRENT_TIMESTAMP(), is_current_row = FALSE
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        address_hk, address_id, customer_uid_fk, flat_no, house_no, floor_label,
        tower_block, near_landmark, area_locality, city_name, state_name, postal_code,
        geo_coords, is_primary, address_kind, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.address_id, s.customer_uid_fk, s.flat_no, s.house_no, s.floor_label,
            s.tower_block, s.near_landmark, s.area_locality, s.city_name, s.state_name,
            s.postal_code, s.geo_coords, s.is_primary, s.address_kind
        ))),
        s.address_id, s.customer_uid_fk, s.flat_no, s.house_no, s.floor_label, s.tower_block,
        s.near_landmark, s.area_locality, s.city_name, s.state_name, s.postal_code,
        s.geo_coords, s.is_primary, s.address_kind, CURRENT_TIMESTAMP(), NULL, TRUE
    )
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        address_hk, address_id, customer_uid_fk, flat_no, house_no, floor_label,
        tower_block, near_landmark, area_locality, city_name, state_name, postal_code,
        geo_coords, is_primary, address_kind, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.address_id, s.customer_uid_fk, s.flat_no, s.house_no, s.floor_label,
            s.tower_block, s.near_landmark, s.area_locality, s.city_name, s.state_name,
            s.postal_code, s.geo_coords, s.is_primary, s.address_kind
        ))),
        s.address_id, s.customer_uid_fk, s.flat_no, s.house_no, s.floor_label, s.tower_block,
        s.near_landmark, s.area_locality, s.city_name, s.state_name, s.postal_code,
        s.geo_coords, s.is_primary, s.address_kind, CURRENT_TIMESTAMP(), NULL, TRUE
    );
