USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.ORDER_HEADER_SRC (
    orderid TEXT,
    customerid TEXT,
    restaurantid TEXT,
    orderdate TEXT,
    totalamount TEXT,
    status TEXT,
    paymentmethod TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.ORDER_HEADER_SRC_STREAM
    ON TABLE LANDING.ORDER_HEADER_SRC
    APPEND_ONLY = TRUE;

COPY INTO LANDING.ORDER_HEADER_SRC (
    orderid, customerid, restaurantid, orderdate, totalamount, status, paymentmethod,
    createddate, modifieddate, _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY, CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/orders/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.FACT_ORDER_HEADER (
    order_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    order_id NUMBER NOT NULL UNIQUE,
    customer_uid_fk VARCHAR(40),
    restaurant_id_fk NUMBER,
    ordered_at TIMESTAMP_NTZ,
    order_total_inr DECIMAL(14, 2),
    lifecycle_status VARCHAR(40),
    pay_instrument VARCHAR(40),
    created_ts TIMESTAMP_TZ,
    updated_ts TIMESTAMP_TZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.FACT_ORDER_HEADER_STREAM ON TABLE REFINED.FACT_ORDER_HEADER;

MERGE INTO REFINED.FACT_ORDER_HEADER AS tgt
USING (
    SELECT
        TRY_TO_NUMBER(o.orderid) AS order_id,
        TRIM(o.customerid) AS customer_uid_fk,
        TRY_TO_NUMBER(o.restaurantid) AS restaurant_id_fk,
        TRY_TO_TIMESTAMP_NTZ(o.orderdate) AS ordered_at,
        TRY_TO_DECIMAL(o.totalamount, 14, 2) AS order_total_inr,
        TRIM(o.status) AS lifecycle_status,
        TRIM(o.paymentmethod) AS pay_instrument,
        TRY_TO_TIMESTAMP_TZ(o.createddate) AS created_ts,
        TRY_TO_TIMESTAMP_TZ(o.modifieddate) AS updated_ts,
        o._src_file, o._src_file_ts, o._src_file_key
    FROM LANDING.ORDER_HEADER_SRC_STREAM o
) AS src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN
    UPDATE SET
        order_total_inr = src.order_total_inr,
        lifecycle_status = src.lifecycle_status,
        pay_instrument = src.pay_instrument,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        order_id, customer_uid_fk, restaurant_id_fk, ordered_at, order_total_inr,
        lifecycle_status, pay_instrument, created_ts, updated_ts,
        _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.order_id, src.customer_uid_fk, src.restaurant_id_fk, src.ordered_at,
        src.order_total_inr, src.lifecycle_status, src.pay_instrument, src.created_ts,
        src.updated_ts, src._src_file, src._src_file_ts, src._src_file_key
    );
