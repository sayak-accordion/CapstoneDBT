
WITH numberedStore AS(
    SELECT 
        *,
        ROW_NUMBER()OVER (
            PARTITION BY store_id
            ORDER BY store_id
        ) AS rn
    FROM {{source('raw','store_raw')}}
)
SELECT 
    STORE_ID, 
    STORE_NAME,
    STORE_TYPE,
    REGION,
    MANAGER_ID,
    EMAIL,
    PHONE_NUMBER,
    IS_ACTIVE,
    EMPLOYEE_COUNT,
    SIZE_SQ_FT,
    CURRENT_SALES,
    SALES_TARGET,
    MONTHLY_RENT,
    OPENING_DATE,
    LAST_MODIFIED_DATE,
    ADDRESS_STREET,
    ADDRESS_CITY,
    ADDRESS_STATE,
    ADDRESS_COUNTRY,
    ADDRESS_ZIP_CODE,
    OPERATING_HOURS_WEEKDAYS,
    OPERATING_HOURS_WEEKENDS,
    OPERATING_HOURS_HOLIDAYS,
    SERVICES,
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    NumberedStore
WHERE
    rn = 1
