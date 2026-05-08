USE ROLE SYSADMIN;
USE WAREHOUSE FOOD_ETL_WH;
USE DATABASE FOOD_PLATFORM_DW;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.FACT_ORDER_LINE (
    order_line_fact_sk NUMBER AUTOINCREMENT,
    order_line_id NUMBER,
    order_id NUMBER,
    customer_dim_key NUMBER,
    customer_address_dim_key NUMBER,
    restaurant_dim_key NUMBER,
    restaurant_location_dim_key NUMBER,
    menu_dim_key NUMBER,
    delivery_agent_dim_key NUMBER,
    order_date_dim_key NUMBER,
    quantity NUMBER(12, 2),
    price NUMBER(12, 2),
    subtotal NUMBER(14, 2),
    delivery_status VARCHAR(80),
    estimated_time VARCHAR(120)
)
COMMENT = 'Line-grain mart tying orders to role-playing dimensions.';

MERGE INTO ANALYTICS.FACT_ORDER_LINE AS tgt
USING (
    SELECT
        oi.order_line_id,
        oi.order_id_fk AS order_id,
        c.customer_hk AS customer_dim_key,
        ca.address_hk AS customer_address_dim_key,
        r.restaurant_hk AS restaurant_dim_key,
        loc.location_hk AS restaurant_location_dim_key,
        m.menu_dim_hk AS menu_dim_key,
        da.delivery_agent_hk AS delivery_agent_dim_key,
        dd.date_dim_hk AS order_date_dim_key,
        oi.qty AS quantity,
        oi.unit_price_inr AS price,
        oi.line_total_inr AS subtotal,
        d.ship_status AS delivery_status,
        d.eta_text AS estimated_time
    FROM REFINED.FACT_ORDER_LINE_STREAM oi
    INNER JOIN REFINED.FACT_ORDER_HEADER o
        ON oi.order_id_fk = o.order_id
    INNER JOIN REFINED.FACT_DELIVERY_HUB d
        ON o.order_id = d.order_id_fk
    INNER JOIN ANALYTICS.DIM_CUSTOMER c
        ON o.customer_uid_fk = c.customer_id
        AND c.is_current_row
    INNER JOIN ANALYTICS.DIM_CUSTOMER_ADDRESS ca
        ON c.customer_id = ca.customer_uid_fk
        AND ca.is_current_row
        AND UPPER(TRIM(NVL(ca.is_primary, 'N'))) IN ('Y', 'YES', '1', 'TRUE')
    INNER JOIN ANALYTICS.DIM_RESTAURANT r
        ON o.restaurant_id_fk = r.restaurant_id
        AND r.is_current_row
    INNER JOIN ANALYTICS.DIM_LOCATION loc
        ON r.location_id_fk = loc.location_id
        AND loc.is_current_row
    INNER JOIN ANALYTICS.DIM_MENU m
        ON oi.menu_id_fk = m.menu_id
        AND m.is_current_row
    INNER JOIN ANALYTICS.DIM_DELIVERY_AGENT da
        ON d.rider_id_fk = da.delivery_agent_id
        AND da.is_current_row
    INNER JOIN ANALYTICS.DIM_DATE_SPINE dd
        ON dd.calendar_date = DATE(o.ordered_at)
) AS src
ON tgt.order_line_id = src.order_line_id
    AND tgt.order_id = src.order_id
WHEN MATCHED THEN
    UPDATE SET
        customer_dim_key = src.customer_dim_key,
        customer_address_dim_key = src.customer_address_dim_key,
        restaurant_dim_key = src.restaurant_dim_key,
        restaurant_location_dim_key = src.restaurant_location_dim_key,
        menu_dim_key = src.menu_dim_key,
        delivery_agent_dim_key = src.delivery_agent_dim_key,
        order_date_dim_key = src.order_date_dim_key,
        quantity = src.quantity,
        price = src.price,
        subtotal = src.subtotal,
        delivery_status = src.delivery_status,
        estimated_time = src.estimated_time
WHEN NOT MATCHED THEN
    INSERT (
        order_line_id, order_id, customer_dim_key, customer_address_dim_key,
        restaurant_dim_key, restaurant_location_dim_key, menu_dim_key, delivery_agent_dim_key,
        order_date_dim_key, quantity, price, subtotal, delivery_status, estimated_time
    )
    VALUES (
        src.order_line_id, src.order_id, src.customer_dim_key, src.customer_address_dim_key,
        src.restaurant_dim_key, src.restaurant_location_dim_key, src.menu_dim_key,
        src.delivery_agent_dim_key, src.order_date_dim_key, src.quantity, src.price,
        src.subtotal, src.delivery_status, src.estimated_time
    );

-- Optional referential integrity (enable after validating dimension coverage)
-- ALTER TABLE ANALYTICS.FACT_ORDER_LINE ADD CONSTRAINT fk_fact_customer ...

CREATE OR REPLACE VIEW ANALYTICS.VW_YEARLY_REVENUE_KPIS AS
SELECT
    d.year_nbr AS year,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(f.order_line_id), 0), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM ANALYTICS.FACT_ORDER_LINE f
INNER JOIN ANALYTICS.DIM_DATE_SPINE d
    ON f.order_date_dim_key = d.date_dim_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.year_nbr
ORDER BY d.year_nbr;

CREATE OR REPLACE VIEW ANALYTICS.VW_MONTHLY_REVENUE_KPIS AS
SELECT
    d.year_nbr AS year,
    d.month_nbr AS month,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(f.order_line_id), 0), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM ANALYTICS.FACT_ORDER_LINE f
INNER JOIN ANALYTICS.DIM_DATE_SPINE d
    ON f.order_date_dim_key = d.date_dim_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.year_nbr, d.month_nbr
ORDER BY d.year_nbr, d.month_nbr;

CREATE OR REPLACE VIEW ANALYTICS.VW_DAILY_REVENUE_KPIS AS
SELECT
    d.year_nbr AS year,
    d.month_nbr AS month,
    d.day_of_month AS day,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(f.order_line_id), 0), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM ANALYTICS.FACT_ORDER_LINE f
INNER JOIN ANALYTICS.DIM_DATE_SPINE d
    ON f.order_date_dim_key = d.date_dim_hk
WHERE f.delivery_status = 'Delivered'
GROUP BY d.year_nbr, d.month_nbr, d.day_of_month
ORDER BY d.year_nbr, d.month_nbr, d.day_of_month;

CREATE OR REPLACE VIEW ANALYTICS.VW_MONTHLY_REVENUE_BY_RESTAURANT AS
SELECT
    d.year_nbr AS year,
    d.month_nbr AS month,
    f.delivery_status,
    r.brand_name AS restaurant_name,
    SUM(f.subtotal) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_revenue_per_order,
    ROUND(SUM(f.subtotal) / NULLIF(COUNT(f.order_line_id), 0), 2) AS avg_revenue_per_item,
    MAX(f.subtotal) AS max_order_value
FROM ANALYTICS.FACT_ORDER_LINE f
INNER JOIN ANALYTICS.DIM_DATE_SPINE d
    ON f.order_date_dim_key = d.date_dim_hk
INNER JOIN ANALYTICS.DIM_RESTAURANT r
    ON f.restaurant_dim_key = r.restaurant_hk
GROUP BY d.year_nbr, d.month_nbr, f.delivery_status, r.brand_name
ORDER BY d.year_nbr, d.month_nbr;
