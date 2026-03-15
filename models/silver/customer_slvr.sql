SELECT
    CUSTOMER_ID, 
    UPPER(FIRST_NAME) AS first_name, 
    UPPER(LAST_NAME) AS last_name, 
    UPPER(CONCAT(FIRST_NAME, ' ', LAST_NAME)) AS full_name,
    EMAIL,
    (
    CASE 
        WHEN EMAIL LIKE '_%@_%._%' THEN TRUE
        ELSE FALSE
    END
    ) AS EMAIL_VALIDATION,
    {{source('bronze','standardisePhone')}}(PHONE) AS phone,
    UPPER(OCCUPATION) AS occupation, 
    UPPER(INCOME_BRACKET) AS income_bracket, 
    UPPER(LOYALTY_TIER) AS loyalty_tier, 
    UPPER(PREFERRED_COMMUNICATION) AS preferred_communication,
    UPPER(PREFERRED_PAYMENT_METHOD) AS preferred_payment_method, 
    MARKETING_OPT_IN , 
    BIRTH_DATE,
    DATEDIFF(year,COALESCE(BIRTH_DATE,CURRENT_DATE()),CURRENT_DATE()) AS Age,
    (CASE
        WHEN Age>17 AND Age<36 THEN 'YOUNG'
        WHEN Age>35 AND Age<56 THEN 'MIDDLE-AGED'
        WHEN Age>55 THEN 'SENIOR'
        ELSE 'KID'
    END) AS Customer_Segment,
    REGISTRATION_DATE, 
    LAST_PURCHASE_DATE,
    LAST_MODIFIED_DATE,
    TOTAL_PURCHASES,
    TOTAL_SPEND,
    UPPER(STREET) AS street,
    UPPER(CITY) AS city,
    UPPER(STATE) AS state,
    UPPER(COUNTRY) AS country,
    ZIP_CODE,
    ------------
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM {{source('bronze','customer_brnz')}}
