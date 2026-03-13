WITH numberedOrders AS(
    SELECT 
        *,
        ROW_NUMBER()OVER (
            PARTITION BY order_id
            ORDER BY order_id
        ) AS rn
    FROM {{source('raw', 'orders_raw')}}
)
SELECT 
    ORDER_ID, 
    CUSTOMER_ID, 
    EMPLOYEE_ID, 
    CAMPAIGN_ID, 
    STORE_ID, 
    ORDER_SOURCE, 
    ORDER_STATUS, 
    PAYMENT_METHOD, 
    SHIPPING_METHOD, 
    CREATED_AT, 
    ORDER_DATE,
    SHIPPING_DATE,
    DELIVERY_DATE,
    ESTIMATED_DELIVERY_DATE,
    SHIPPING_COST,
    DISCOUNT_AMOUNT,
    TAX_AMOUNT,
    TOTAL_AMOUNT,
    BILLING_STREET,
    BILLING_CITY,
    BILLING_STATE,
    BILLING_ZIP_CODE,
    SHIPPING_STREET,
    SHIPPING_CITY,
    SHIPPING_STATE,
    SHIPPING_ZIP_CODE,
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    numberedOrders
WHERE
    rn = 1
