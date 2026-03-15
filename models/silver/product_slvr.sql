SELECT 
    PRODUCT_ID,
    {{source('bronze','convert2Pascal')}}(PRODUCT_NAME) AS PRODUCT_NAME,
    UPPER(BRAND) AS brand, 
    {{source('bronze','convert2Pascal')}}(CATEGORY) AS category,
    UPPER(SUBCATEGORY) AS subcategory,
    UPPER(PRODUCT_LINE) AS product_line,
    UPPER(COLOR) AS color,
    UPPER(SIZE) AS size, 
    UPPER(SHORT_DESCRIPTION) AS short_description,
    UPPER(TECHNICAL_SPECS) AS technical_specs,
    CONCAT(PRODUCT_NAME,' ',short_description,' ', technical_specs) AS full_description,
    SUPPLIER_ID, 
    DIMENSIONS,
    CAST(SUBSTR(WEIGHT,0,LENGTH(WEIGHT)-3) AS FLOAT) AS weight_kg,
    CASE
        WHEN SUBSTR(WARRANTY_PERIOD,0,CHARINDEX(WARRANTY_PERIOD, ' ')+2)='No' THEN 0
        ELSE CAST(SUBSTR(WARRANTY_PERIOD,0,CHARINDEX(WARRANTY_PERIOD, ' ')+2) AS FLOAT)
    END
    AS warranty_period_yrs,
    IS_FEATURED,
    LAUNCH_DATE,
    LAST_MODIFIED_DATE,
    STOCK_QUANTITY,
    REORDER_LEVEL,
    CASE
        WHEN STOCK_QUANTITY < REORDER_LEVEL THEN TRUE
        ELSE FALSE
    END
    AS is_stock_low,
    COST_PRICE,
    UNIT_PRICE,
    CAST((UNIT_PRICE-COST_PRICE)*100/UNIT_PRICE AS DECIMAL(10,2)) AS profit_margin,
    -------------
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    {{source('bronze', 'product_brnz')}}
