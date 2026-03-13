WITH numberedCustomer AS(
    SELECT 
        *,
        ROW_NUMBER()OVER (
            PARTITION BY customer_id
            ORDER BY customer_id
        ) AS rn
    FROM raw.customer_raw
)
SELECT 
    CUSTOMER_ID, 
    FIRST_NAME,
    LAST_NAME, 
    EMAIL, 
    PHONE, 
    OCCUPATION, 
    INCOME_BRACKET, 
    LOYALTY_TIER, 
    PREFERRED_COMMUNICATION, 
    PREFERRED_PAYMENT_METHOD, 
    MARKETING_OPT_IN, 
    BIRTH_DATE, 
    REGISTRATION_DATE, 
    LAST_PURCHASE_DATE, 
    LAST_MODIFIED_DATE, 
    TOTAL_PURCHASES, 
    TOTAL_SPEND, 
    STREET, 
    CITY, 
    STATE, 
    COUNTRY, 
    ZIP_CODE, 
    RECORD_HASH, 
    EFFECTIVE_FROM,
    EFFECTIVE_TO, 
    IS_CURRENT, 
    LOAD_TIMESTAMP,
    FILENAME, 
    FILE_ROW_NUMBER
FROM
    numberedCustomer
WHERE
    rn = 1
