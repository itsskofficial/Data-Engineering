USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.ORDER_LINE_SRC (
    orderitemid TEXT,
    orderid TEXT,
    menuid TEXT,
    quantity TEXT,
    price TEXT,
    subtotal TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.ORDER_LINE_SRC_STREAM ON TABLE LANDING.ORDER_LINE_SRC APPEND_ONLY = TRUE;

COPY INTO LANDING.ORDER_LINE_SRC (
    orderitemid, orderid, menuid, quantity, price, subtotal, createddate, modifieddate,
    _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY, CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/order-items/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.FACT_ORDER_LINE (
    order_line_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    order_line_id NUMBER NOT NULL UNIQUE,
    order_id_fk NUMBER NOT NULL,
    menu_id_fk NUMBER NOT NULL,
    qty NUMBER(12, 2),
    unit_price_inr NUMBER(12, 2),
    line_total_inr NUMBER(14, 2),
    created_ts TIMESTAMP_NTZ,
    updated_ts TIMESTAMP_NTZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.FACT_ORDER_LINE_STREAM ON TABLE REFINED.FACT_ORDER_LINE;

MERGE INTO REFINED.FACT_ORDER_LINE AS tgt
USING (
    SELECT
        TRY_TO_NUMBER(l.orderitemid) AS order_line_id,
        TRY_TO_NUMBER(l.orderid) AS order_id_fk,
        TRY_TO_NUMBER(l.menuid) AS menu_id_fk,
        TRY_TO_DECIMAL(l.quantity, 12, 2) AS qty,
        TRY_TO_DECIMAL(l.price, 12, 2) AS unit_price_inr,
        TRY_TO_DECIMAL(l.subtotal, 14, 2) AS line_total_inr,
        TRY_TO_TIMESTAMP_NTZ(l.createddate) AS created_ts,
        TRY_TO_TIMESTAMP_NTZ(l.modifieddate) AS updated_ts,
        l._src_file, l._src_file_ts, l._src_file_key
    FROM LANDING.ORDER_LINE_SRC_STREAM l
) AS src
ON tgt.order_line_id = src.order_line_id
WHEN MATCHED THEN
    UPDATE SET
        qty = src.qty,
        unit_price_inr = src.unit_price_inr,
        line_total_inr = src.line_total_inr,
        created_ts = src.created_ts,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        order_line_id, order_id_fk, menu_id_fk, qty, unit_price_inr, line_total_inr,
        created_ts, updated_ts, _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.order_line_id, src.order_id_fk, src.menu_id_fk, src.qty, src.unit_price_inr,
        src.line_total_inr, src.created_ts, src.updated_ts, src._src_file, src._src_file_ts,
        src._src_file_key
    );
