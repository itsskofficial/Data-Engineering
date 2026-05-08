USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA ANALYTICS;

-- Date spine from earliest order through today (generator pattern instead of recursive CTE)
CREATE OR REPLACE TABLE ANALYTICS.DIM_DATE_SPINE (
    date_dim_hk NUMBER NOT NULL,
    calendar_date DATE NOT NULL,
    year_nbr NUMBER(4, 0) NOT NULL,
    quarter_nbr NUMBER(1, 0) NOT NULL,
    month_nbr NUMBER(2, 0) NOT NULL,
    week_of_year NUMBER(2, 0) NOT NULL,
    day_of_year NUMBER(3, 0) NOT NULL,
    day_of_week NUMBER(1, 0) NOT NULL,
    day_of_month NUMBER(2, 0) NOT NULL,
    weekday_name VARCHAR(12) NOT NULL,
    PRIMARY KEY (date_dim_hk)
)
COMMENT = 'Calendar support for order facts; hash key stable per calendar_date.';

TRUNCATE TABLE ANALYTICS.DIM_DATE_SPINE;

INSERT INTO ANALYTICS.DIM_DATE_SPINE (
    date_dim_hk,
    calendar_date,
    year_nbr,
    quarter_nbr,
    month_nbr,
    week_of_year,
    day_of_year,
    day_of_week,
    day_of_month,
    weekday_name
)
SELECT
    HASH(SHA1_HEX(TO_VARCHAR(calendar_date))) AS date_dim_hk,
    calendar_date,
    YEAR(calendar_date) AS year_nbr,
    QUARTER(calendar_date) AS quarter_nbr,
    MONTH(calendar_date) AS month_nbr,
    WEEKOFYEAR(calendar_date) AS week_of_year,
    DAYOFYEAR(calendar_date) AS day_of_year,
    DAYOFWEEK(calendar_date) AS day_of_week,
    DAY(calendar_date) AS day_of_month,
    DAYNAME(calendar_date) AS weekday_name
FROM (
    SELECT
        DATEADD(
            'day',
            SEQ4(),
            COALESCE(
                (SELECT MIN(DATE(ordered_at)) FROM REFINED.FACT_ORDER_HEADER),
                DATEADD('year', -5, CURRENT_DATE())
            )
        ) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
) spine
WHERE calendar_date <= CURRENT_DATE();
