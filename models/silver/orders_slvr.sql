WITH order_cost_price AS (
    SELECT 
         ORDER_ID,
         SUM(quantity*cost_price) AS total_cost,
         COUNT(product_id) AS total_items,
         SUM(quantity) AS total_quantity,
    FROM 
        order_product_brnz
    GROUP BY 
        ORDER_ID
)
-- SELECT * FROM order_cost_price ORDER BY ORDER_ID;
SELECT
    o.ORDER_ID, 
    CUSTOMER_ID,
    EMPLOYEE_ID,
    CAMPAIGN_ID,
    STORE_ID,
    UPPER(ORDER_SOURCE) AS order_source,
    UPPER(ORDER_STATUS) AS order_status,
    UPPER(PAYMENT_METHOD) AS payment_method,
    UPPER(SHIPPING_METHOD) AS shipping_method,
    --------------------
    CREATED_AT,
    ORDER_DATE,
    DATE_PART(week, ORDER_DATE)-DATE_PART(week, DATE_TRUNC(month,ORDER_DATE))+1 AS order_week,
    DATE_PART(month, ORDER_DATE) AS order_month,
    DATE_PART(quarter, ORDER_DATE) AS order_quarter,
    DATE_PART(year, ORDER_DATE) AS order_year,
    CASE 
        WHEN CAST(ORDER_DATE AS TIME) >= TO_TIME('05:00:00','HH24:MI:SS')
            AND CAST(ORDER_DATE AS TIME) < TO_TIME('12:00:00','HH24:MI:SS') THEN 'MORNING'
        WHEN CAST(ORDER_DATE AS TIME) >= TO_TIME('12:00:00','HH24:MI:SS')
            AND CAST(ORDER_DATE AS TIME) < TO_TIME('17:00:00','HH24:MI:SS') THEN 'AFTERNOON'
        WHEN  CAST(ORDER_DATE AS TIME) >= TO_TIME('17:00:00','HH24:MI:SS')
            AND CAST(ORDER_DATE AS TIME) < TO_TIME('22:00:00','HH24:MI:SS') THEN 'EVENING'
        ELSE 'NIGHT'
    END
    AS order_time_of_day,
    SHIPPING_DATE,
    DELIVERY_DATE,
    ESTIMATED_DELIVERY_DATE,
    DATEDIFF(day, ORDER_DATE, SHIPPING_DATE) AS processing_days,
    DATEDIFF(day, SHIPPING_DATE, DELIVERY_DATE) AS shipping_days,
    CASE 
        WHEN DELIVERY_DATE IS NOT NULL AND
            DELIVERY_DATE<=ESTIMATED_DELIVERY_DATE THEN 'ON TIME'
        WHEN DELIVERY_DATE IS NOT NULL AND
            DELIVERY_DATE>ESTIMATED_DELIVERY_DATE THEN 'DELAYED'
        WHEN DELIVERY_DATE IS NOT NULL AND
            CURRENT_DATE() > ESTIMATED_DELIVERY_DATE THEN 'POTENTIALLY DELAYED'
        ELSE 'IN TRANSIT'
    END
    AS delivery_status,
    ----------------------
    SHIPPING_COST,
    DISCOUNT_AMOUNT AS total_discount,
    TAX_AMOUNT,
    TOTAL_AMOUNT,
    ocp.total_cost AS total_cost,
    (TOTAL_AMOUNT-total_cost-TAX_AMOUNT-SHIPPING_COST-total_discount) AS profit_amount,
    CAST((profit_amount * 100 / TOTAL_AMOUNT) AS DECIMAL(10,2)) AS profit_margin_percentage,
    ----------------------
    UPPER(BILLING_STREET) AS billing_street,
    UPPER(BILLING_CITY) AS billing_city,
    UPPER(BILLING_STATE) AS billing_state,
    UPPER(BILLING_ZIP_CODE) AS billing_zip_code,
    UPPER(SHIPPING_STREET) AS shipping_street,
    UPPER(SHIPPING_CITY) AS shipping_city,
    UPPER(SHIPPING_STATE) AS shipping_state,
    UPPER(SHIPPING_ZIP_CODE) As shipping_zip_code,
    ------------------------
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    {{source('bronze','orders_brnz')}} o JOIN order_cost_price ocp
    ON o.order_id = ocp.order_id
