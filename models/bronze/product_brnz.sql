
WITH numberedProduct AS(
    SELECT 
        *,
        ROW_NUMBER()OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS rn
    FROM {{source('raw','product_raw')}}
)
SELECT 
    PRODUCT_ID, 
    PRODUCT_NAME,
    BRAND,
    CATEGORY,
    SUBCATEGORY,
    PRODUCT_LINE,
    COLOR,
    SIZE,
    SHORT_DESCRIPTION,
    TECHNICAL_SPECS,
    SUPPLIER_ID,
    DIMENSIONS,
    WEIGHT,
    WARRANTY_PERIOD,
    IS_FEATURED,
    LAUNCH_DATE,
    LAST_MODIFIED_DATE,
    STOCK_QUANTITY,
    REORDER_LEVEL,
    COST_PRICE,
    UNIT_PRICE,
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    NumberedProduct
WHERE
    rn = 1
