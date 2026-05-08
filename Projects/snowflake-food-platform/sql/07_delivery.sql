USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA LANDING;

CREATE OR REPLACE TABLE LANDING.DELIVERY_SRC (
    deliveryid TEXT,
    orderid TEXT,
    deliveryagentid TEXT,
    deliverystatus TEXT,
    estimatedtime TEXT,
    addressid TEXT,
    deliverydate TEXT,
    createddate TEXT,
    modifieddate TEXT,
    _src_file TEXT,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key TEXT,
    _ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM LANDING.DELIVERY_SRC_STREAM ON TABLE LANDING.DELIVERY_SRC APPEND_ONLY = TRUE;

COPY INTO LANDING.DELIVERY_SRC (
    deliveryid, orderid, deliveryagentid, deliverystatus, estimatedtime, addressid,
    deliverydate, createddate, modifieddate, _src_file, _src_file_ts, _src_file_key, _ingested_at
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED, METADATA$FILE_CONTENT_KEY, CURRENT_TIMESTAMP()
    FROM @LANDING.RAW_FILES_STG/initial/delivery/ t
)
FILE_FORMAT = (FORMAT_NAME = 'LANDING.FF_CSV_PIPE')
ON_ERROR = ABORT_STATEMENT;

USE SCHEMA REFINED;

CREATE OR REPLACE TABLE REFINED.FACT_DELIVERY_HUB (
    delivery_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    delivery_id NUMBER NOT NULL,
    order_id_fk NUMBER NOT NULL,
    rider_id_fk NUMBER NOT NULL,
    ship_status VARCHAR(40),
    eta_text VARCHAR(80),
    ship_to_address_id NUMBER NOT NULL,
    delivered_ts TIMESTAMP_NTZ,
    created_ts TIMESTAMP_NTZ,
    updated_ts TIMESTAMP_NTZ,
    _src_file VARCHAR,
    _src_file_ts TIMESTAMP_NTZ,
    _src_file_key VARCHAR,
    _refined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM REFINED.FACT_DELIVERY_HUB_STREAM ON TABLE REFINED.FACT_DELIVERY_HUB;

MERGE INTO REFINED.FACT_DELIVERY_HUB AS tgt
USING (
    SELECT
        TRY_TO_NUMBER(d.deliveryid) AS delivery_id,
        TRY_TO_NUMBER(d.orderid) AS order_id_fk,
        TRY_TO_NUMBER(d.deliveryagentid) AS rider_id_fk,
        TRIM(d.deliverystatus) AS ship_status,
        TRIM(d.estimatedtime) AS eta_text,
        TRY_TO_NUMBER(d.addressid) AS ship_to_address_id,
        TRY_TO_TIMESTAMP_NTZ(d.deliverydate) AS delivered_ts,
        TRY_TO_TIMESTAMP_NTZ(d.createddate) AS created_ts,
        TRY_TO_TIMESTAMP_NTZ(d.modifieddate) AS updated_ts,
        d._src_file, d._src_file_ts, d._src_file_key
    FROM LANDING.DELIVERY_SRC_STREAM d
) AS src
ON tgt.delivery_id = src.delivery_id
    AND tgt.order_id_fk = src.order_id_fk
    AND tgt.rider_id_fk = src.rider_id_fk
WHEN MATCHED THEN
    UPDATE SET
        ship_status = src.ship_status,
        eta_text = src.eta_text,
        ship_to_address_id = src.ship_to_address_id,
        delivered_ts = src.delivered_ts,
        created_ts = src.created_ts,
        updated_ts = src.updated_ts,
        _src_file = src._src_file,
        _src_file_ts = src._src_file_ts,
        _src_file_key = src._src_file_key,
        _refined_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        delivery_id, order_id_fk, rider_id_fk, ship_status, eta_text, ship_to_address_id,
        delivered_ts, created_ts, updated_ts, _src_file, _src_file_ts, _src_file_key
    )
    VALUES (
        src.delivery_id, src.order_id_fk, src.rider_id_fk, src.ship_status, src.eta_text,
        src.ship_to_address_id, src.delivered_ts, src.created_ts, src.updated_ts,
        src._src_file, src._src_file_ts, src._src_file_key
    );
