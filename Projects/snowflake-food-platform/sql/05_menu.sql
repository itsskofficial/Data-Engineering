USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.MENU_SRC (
    menuid TEXT,
    restaurantid TEXT,
    itemname TEXT,
    description TEXT,
    price TEXT,
    category TEXT,
    availability TEXT,
    itemtype TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.MENU_SRC_STREAM ON TABLE LANDING.MENU_SRC APPEND_ONLY = TRUE;

COPY INTO LANDING.MENU_SRC (
    menuid, restaurantid, itemname, description, price, category, availability, itemtype,
    createddate, modifieddate, _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY, CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/menu/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.DIM_MENU_HUB (
    menu_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    menu_id NUMBER NOT NULL UNIQUE,
    restaurant_id_fk NUMBER,
    dish_title VARCHAR(200) NOT NULL,
    dish_blurb VARCHAR(1000),
    list_price_inr DECIMAL(12, 2) NOT NULL,
    menu_section VARCHAR(80),
    is_offered BOOLEAN,
    diet_tag VARCHAR(40),
    created_ts TIMESTAMP_NTZ,
    updated_ts TIMESTAMP_NTZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.DIM_MENU_HUB_STREAM ON TABLE REFINED.DIM_MENU_HUB;

MERGE INTO REFINED.DIM_MENU_HUB AS tgt
USING (
    SELECT
        TRY_TO_NUMBER(m.menuid) AS menu_id,
        TRY_TO_NUMBER(m.restaurantid) AS restaurant_id_fk,
        TRIM(m.itemname) AS dish_title,
        TRIM(m.description) AS dish_blurb,
        TRY_TO_DECIMAL(m.price, 12, 2) AS list_price_inr,
        TRIM(m.category) AS menu_section,
        IFF(LOWER(TRIM(m.availability)) = 'true', TRUE,
            IFF(LOWER(TRIM(m.availability)) = 'false', FALSE, NULL)) AS is_offered,
        TRIM(m.itemtype) AS diet_tag,
        TRY_CAST(m.createddate AS TIMESTAMP_NTZ) AS created_ts,
        TRY_CAST(m.modifieddate AS TIMESTAMP_NTZ) AS updated_ts,
        m._src_file, m._src_file_ts, m._src_file_key
    FROM LANDING.MENU_SRC_STREAM m
) AS src
ON tgt.menu_id = src.menu_id
WHEN MATCHED THEN
    UPDATE SET
        restaurant_id_fk = src.restaurant_id_fk,
        dish_title = src.dish_title,
        dish_blurb = src.dish_blurb,
        list_price_inr = src.list_price_inr,
        menu_section = src.menu_section,
        is_offered = src.is_offered,
        diet_tag = src.diet_tag,
        created_ts = src.created_ts,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        menu_id, restaurant_id_fk, dish_title, dish_blurb, list_price_inr, menu_section,
        is_offered, diet_tag, created_ts, updated_ts, _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.menu_id, src.restaurant_id_fk, src.dish_title, src.dish_blurb, src.list_price_inr,
        src.menu_section, src.is_offered, src.diet_tag, src.created_ts, src.updated_ts,
        src._src_file, src._src_file_ts, src._src_file_key
    );

USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_MENU (
    menu_dim_hk NUMBER PRIMARY KEY,
    menu_id NUMBER NOT NULL,
    restaurant_id_fk NUMBER NOT NULL,
    dish_title VARCHAR(200),
    dish_blurb VARCHAR(1000),
    list_price_inr DECIMAL(12, 2),
    menu_section VARCHAR(80),
    is_offered BOOLEAN,
    diet_tag VARCHAR(40),
    eff_start_ts TIMESTAMP_NTZ,
    eff_end_ts TIMESTAMP_NTZ,
    is_current_row BOOLEAN
);

MERGE INTO ANALYTICS.DIM_MENU AS d
USING REFINED.DIM_MENU_HUB_STREAM AS s
ON d.menu_id = s.menu_id
    AND d.restaurant_id_fk = s.restaurant_id_fk
    AND NVL(d.dish_title, '') = NVL(s.dish_title, '')
    AND NVL(d.dish_blurb, '') = NVL(s.dish_blurb, '')
    AND NVL(d.list_price_inr, 0) = NVL(s.list_price_inr, 0)
    AND NVL(d.menu_section, '') = NVL(s.menu_section, '')
    AND NVL(d.is_offered, FALSE) = NVL(s.is_offered, FALSE)
    AND NVL(d.diet_tag, '') = NVL(s.diet_tag, '')
WHEN MATCHED
    AND s.METADATA$ACTION = 'DELETE'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET eff_end_ts = CURRENT_TIMESTAMP(), is_current_row = FALSE
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        menu_dim_hk, menu_id, restaurant_id_fk, dish_title, dish_blurb, list_price_inr,
        menu_section, is_offered, diet_tag, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.menu_id, s.restaurant_id_fk, s.dish_title, s.dish_blurb, s.list_price_inr,
            s.menu_section, s.is_offered, s.diet_tag
        ))),
        s.menu_id, s.restaurant_id_fk, s.dish_title, s.dish_blurb, s.list_price_inr,
        s.menu_section, s.is_offered, s.diet_tag, CURRENT_TIMESTAMP(), NULL, TRUE
    )
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        menu_dim_hk, menu_id, restaurant_id_fk, dish_title, dish_blurb, list_price_inr,
        menu_section, is_offered, diet_tag, eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.menu_id, s.restaurant_id_fk, s.dish_title, s.dish_blurb, s.list_price_inr,
            s.menu_section, s.is_offered, s.diet_tag
        ))),
        s.menu_id, s.restaurant_id_fk, s.dish_title, s.dish_blurb, s.list_price_inr,
        s.menu_section, s.is_offered, s.diet_tag, CURRENT_TIMESTAMP(), NULL, TRUE
    );
