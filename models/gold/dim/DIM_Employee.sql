
SELECT 
    UUID_STRING(
        'efc3a943-a363-4dc4-847e-06b8b0cfddc5',
        EMPLOYEE_ID 
    ) AS EmployeeKey,
    EMPLOYEE_ID AS EmployeeID,
    FULL_NAME AS FullName,
    EMAIL AS Email,
    EMAIL_VALIDATION AS EmailValidation,
    PHONE AS Phone, 
    ROLE AS Role, 
    WORK_LOCATION AS WorkLocation, 
    TENURE_YRS AS Tenure,
    PERFORMANCE_RATING AS PerformnceRating, 
    TARGET_ACHIEVEMENT_PERCENTAGE AS TargetAcheivementPercentage,
    ORDERS_PROCESSED AS OrdersProcessed,
    TOTAL_SALES_AMOUNT AS TotalSalesAmount
FROM
    {{source('silver','employee_slvr')}}
