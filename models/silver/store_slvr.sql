
SELECT 
    STORE_ID, 
    {{source('bronze','convert2Pascal')}}(STORE_NAME) AS store_name,
    UPPER(STORE_TYPE) AS store_type,
    UPPER(REGION) AS region,
    MANAGER_ID,
    EMAIL,
    PHONE_NUMBER,
    IS_ACTIVE,
    EMPLOYEE_COUNT,
    SIZE_SQ_FT,
    CASE
        WHEN SIZE_SQ_FT<5000 THEN 'SMALL'
        WHEN SIZE_SQ_FT BETWEEN 5000 AND 10000 THEN 'MEDIUM'
        WHEN SIZE_SQ_FT > 10000 THEN 'LARGE'
    END
    AS size_category,
    CURRENT_SALES,
    SALES_TARGET,
    CAST((CURRENT_SALES*100)/SALES_TARGET AS DECIMAL(10,2)) AS sales_target_achievement_percentage,
    CAST(CURRENT_SALES/SIZE_SQ_FT AS DECIMAL(10,2)) AS revenue_per_sq_ft,
    CAST(CURRENT_SALES/EMPLOYEE_COUNT AS DECIMAL(10,2)) AS employee_efficiency,
    IFF(sales_target_achievement_percentage<90, TRUE, FALSE) AS performance_issues,
    MONTHLY_RENT,
    OPENING_DATE,
    DATEDIFF(year,OPENING_DATE,CURRENT_DATE()) AS store_age_yrs,
    LAST_MODIFIED_DATE,
    UPPER(ADDRESS_STREET) AS address_street,
    UPPER(ADDRESS_CITY) AS address_city,
    UPPER(ADDRESS_STATE) AS address_state,
    UPPER(ADDRESS_COUNTRY) AS address_country,
    ADDRESS_ZIP_CODE,
    OPERATING_HOURS_WEEKDAYS,
    OPERATING_HOURS_WEEKENDS,
    OPERATING_HOURS_HOLIDAYS,
    SERVICES,
    -------------------
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM 
    {{source('bronze','store_brnz')}}
