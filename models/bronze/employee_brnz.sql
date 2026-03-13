WITH numberedEmployee AS(
    SELECT 
        *,
        ROW_NUMBER()OVER (
            PARTITION BY employee_id
            ORDER BY employee_id
        ) AS rn
    FROM {{source('raw', 'employee_raw')}}
)
SELECT 
    EMPLOYEE_ID, 
    FIRST_NAME, 
    LAST_NAME,
    EMAIL, 
    PHONE, 
    ROLE, 
    DEPARTMENT, 
    EDUCATION, 
    EMPLOYMENT_STATUS, 
    MANAGER_ID, 
    WORK_LOCATION, 
    DATE_OF_BIRTH, 
    HIRE_DATE, 
    LAST_MODIFIED_DATE, 
    SALARY, 
    CURRENT_SALES, 
    SALES_TARGET, 
    PERFORMANCE_RATING, 
    ADDRESS_STREET, 
    ADDRESS_CITY, 
    ADDRESS_STATE, 
    ADDRESS_ZIP_CODE, 
    CERTIFICATIONS, 
    RECORD_HASH, 
    EFFECTIVE_FROM, 
    EFFECTIVE_TO, 
    IS_CURRENT, 
    LOAD_TIMESTAMP, 
    FILENAME, 
    FILE_ROW_NUMBER
FROM
    numberedEmployee
WHERE
    rn = 1
