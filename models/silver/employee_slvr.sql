
WITH employee_sales_data AS (
    SELECT 
        EMPLOYEE_ID,
        COUNT(order_id) AS orders_processed,
        SUM(total_amount) AS total_sales_amount
    FROM 
        orders_brnz
    GROUP BY
        EMPLOYEE_ID
)
SELECT 
    e.EMPLOYEE_ID, 
    UPPER(FIRST_NAME) AS first_name,
    UPPER(LAST_NAME) AS last_name,
    UPPER(CONCAT(first_name,' ',last_name)) AS full_name,
    EMAIL,
    (
    CASE 
        WHEN EMAIL LIKE '_%@_%._%' THEN TRUE
        ELSE FALSE
    END
    ) AS EMAIL_VALIDATION,
    PHONE,
    CASE
        WHEN UPPER(ROLE)='STORE MANAGER' THEN 'MANAGER'
        WHEN UPPER(ROLE)='SALES ASSOCIATE' THEN 'ASSOCIATE'
        ELSE UPPER(ROLE)
    END AS ROLE,
    DATE_OF_BIRTH,
    UPPER(EDUCATION) AS education,
    -------------------------
    UPPER(DEPARTMENT)  AS department,
    UPPER(EMPLOYMENT_STATUS) AS employment_status,
    MANAGER_ID,
    WORK_LOCATION,
    HIRE_DATE,
    DATEDIFF(year, HIRE_DATE, current_date()) AS tenure_yrs,
    LAST_MODIFIED_DATE,
    SALARY,
    CURRENT_SALES,
    SALES_TARGET,
    PERFORMANCE_RATING,
    CAST(CURRENT_SALES*100/SALES_TARGET AS DECIMAL(10,2)) AS target_achievement_percentage,
    esd.orders_processed AS orders_processed,
    esd.total_sales_amount AS total_sales_amount,
    ------------------
    UPPER(ADDRESS_STREET) AS address_street,
    UPPER(ADDRESS_CITY) AS address_city,
    UPPER(ADDRESS_STATE) AS address_state,
    ADDRESS_ZIP_CODE,
    -------------------
    CERTIFICATIONS,
    -------------------
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    {{source('bronze','employee_brnz')}} e JOIN employee_sales_data esd
    ON e.employee_id = esd.employee_id
