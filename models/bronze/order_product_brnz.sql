
WITH numberedOrderProduct AS(
    SELECT 
        *,
        ROW_NUMBER()OVER (
            PARTITION BY order_id, product_id
            ORDER BY order_id, product_id
        ) AS rn
    FROM {{source('raw','order_product_raw')}}
)
SELECT 
    ORDER_ID, 
    PRODUCT_ID,
    QUANTITY,
    UNIT_PRICE,
    COST_PRICE,
    DISCOUNT_AMOUNT,
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    NumberedOrderProduct
WHERE
    rn = 1
