USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.CUSTOMER_SRC (
    customerid TEXT,
    name TEXT,
    mobile TEXT WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    email TEXT WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'EMAIL'),
    loginbyusing TEXT,
    gender TEXT WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    dob TEXT WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    anniversary TEXT,
    preferences TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.CUSTOMER_SRC_STREAM ON TABLE LANDING.CUSTOMER_SRC APPEND_ONLY = TRUE;

COPY INTO LANDING.CUSTOMER_SRC (
    customerid, name, mobile, email, loginbyusing, gender, dob, anniversary,
    preferences, createddate, modifieddate, _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY, CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/customer/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.DIM_CUSTOMER_HUB (
    customer_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_uid VARCHAR(40) NOT NULL UNIQUE,
    full_name VARCHAR(120) NOT NULL,
    phone VARCHAR(20) WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    email_addr VARCHAR(120) WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'EMAIL'),
    auth_channel VARCHAR(50),
    gender_code VARCHAR(10) WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    birth_date DATE WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    anniversary_date DATE,
    prefs_text VARCHAR,
    created_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts TIMESTAMP_TZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.DIM_CUSTOMER_HUB_STREAM ON TABLE REFINED.DIM_CUSTOMER_HUB;

MERGE INTO REFINED.DIM_CUSTOMER_HUB AS tgt
USING (
    SELECT
        TRIM(customerid) AS customer_uid,
        TRIM(name) AS full_name,
        TRIM(mobile) AS phone,
        TRIM(email) AS email_addr,
        TRIM(loginbyusing) AS auth_channel,
        TRIM(gender) AS gender_code,
        TRY_TO_DATE(dob, 'YYYY-MM-DD') AS birth_date,
        TRY_TO_DATE(anniversary, 'YYYY-MM-DD') AS anniversary_date,
        TRIM(preferences) AS prefs_text,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS created_ts,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS updated_ts,
        _src_file, _src_file_ts, _src_file_key
    FROM LANDING.CUSTOMER_SRC_STREAM
) AS src
ON tgt.customer_uid = src.customer_uid
WHEN MATCHED THEN
    UPDATE SET
        full_name = src.full_name,
        phone = src.phone,
        email_addr = src.email_addr,
        auth_channel = src.auth_channel,
        gender_code = src.gender_code,
        birth_date = src.birth_date,
        anniversary_date = src.anniversary_date,
        prefs_text = src.prefs_text,
        created_ts = src.created_ts,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        customer_uid, full_name, phone, email_addr, auth_channel, gender_code,
        birth_date, anniversary_date, prefs_text, created_ts, updated_ts,
        _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.customer_uid, src.full_name, src.phone, src.email_addr, src.auth_channel,
        src.gender_code, src.birth_date, src.anniversary_date, src.prefs_text,
        src.created_ts, src.updated_ts, src._src_file, src._src_file_ts, src._src_file_key
    );

USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DIM_CUSTOMER (
    customer_hk NUMBER PRIMARY KEY,
    customer_id VARCHAR(40) NOT NULL,
    full_name VARCHAR(120) NOT NULL,
    phone VARCHAR(20) WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    email_addr VARCHAR(120) WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'EMAIL'),
    auth_channel VARCHAR(50),
    gender_code VARCHAR(10) WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    birth_date DATE WITH TAG (GOVERNANCE.SENSITIVITY_TAG = 'PII'),
    anniversary_date DATE,
    prefs_text VARCHAR,
    eff_start_ts TIMESTAMP_TZ,
    eff_end_ts TIMESTAMP_TZ,
    is_current_row BOOLEAN
);

MERGE INTO ANALYTICS.DIM_CUSTOMER AS d
USING REFINED.DIM_CUSTOMER_HUB_STREAM AS s
ON d.customer_id = s.customer_uid
    AND NVL(d.full_name, '') = NVL(s.full_name, '')
    AND NVL(d.phone, '') = NVL(s.phone, '')
    AND NVL(d.email_addr, '') = NVL(s.email_addr, '')
    AND NVL(d.auth_channel, '') = NVL(s.auth_channel, '')
    AND NVL(d.gender_code, '') = NVL(s.gender_code, '')
    AND NVL(d.birth_date, '1900-01-01') = NVL(s.birth_date, '1900-01-01')
    AND NVL(d.anniversary_date, '1900-01-01') = NVL(s.anniversary_date, '1900-01-01')
    AND NVL(d.prefs_text, '') = NVL(s.prefs_text, '')
WHEN MATCHED
    AND s.METADATA$ACTION = 'DELETE'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET eff_end_ts = CURRENT_TIMESTAMP(), is_current_row = FALSE
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        customer_hk, customer_id, full_name, phone, email_addr, auth_channel,
        gender_code, birth_date, anniversary_date, prefs_text,
        eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.customer_uid, s.full_name, s.phone, s.email_addr, s.auth_channel,
            s.gender_code, s.birth_date, s.anniversary_date, s.prefs_text
        ))),
        s.customer_uid, s.full_name, s.phone, s.email_addr, s.auth_channel,
        s.gender_code, s.birth_date, s.anniversary_date, s.prefs_text,
        CURRENT_TIMESTAMP(), NULL, TRUE
    )
WHEN NOT MATCHED
    AND s.METADATA$ACTION = 'INSERT'
    AND s.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        customer_hk, customer_id, full_name, phone, email_addr, auth_channel,
        gender_code, birth_date, anniversary_date, prefs_text,
        eff_start_ts, eff_end_ts, is_current_row
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            s.customer_uid, s.full_name, s.phone, s.email_addr, s.auth_channel,
            s.gender_code, s.birth_date, s.anniversary_date, s.prefs_text
        ))),
        s.customer_uid, s.full_name, s.phone, s.email_addr, s.auth_channel,
        s.gender_code, s.birth_date, s.anniversary_date, s.prefs_text,
        CURRENT_TIMESTAMP(), NULL, TRUE
    );

ALTER TABLE ANALYTICS.DIM_CUSTOMER ALTER COLUMN phone SET MASKING POLICY GOVERNANCE.MASK_PHONE;
ALTER TABLE ANALYTICS.DIM_CUSTOMER ALTER COLUMN email_addr SET MASKING POLICY GOVERNANCE.MASK_EMAIL;
