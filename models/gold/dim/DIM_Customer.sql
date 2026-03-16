SELECT 
    UUID_STRING(
        'efc3a943-a363-4dc4-847e-06b8b0cfddc5',
        CUSTOMER_ID 
    ) AS CustomerKey,
    CUSTOMER_ID AS CustomerID,
    FULL_NAME AS FullName,
    EMAIL AS Email,
    EMAIL_VALIDATION AS EmailValidation,
    PHONE As Phone,
    OCCUPATION AS Occupation, 
    INCOME_BRACKET AS IncomeBracket,
    AGE AS Age,
    TOTAL_PURCHASES AS TotalPurchases,
    TOTAL_SPEND AS TotalSpend,
    CUSTOMER_SEGMENT AS CustomerSegment,
    REGISTRATION_DATE AS RegistrationDate,
    LAST_MODIFIED_DATE AS LastModifiedDate,   
    STREET AS Street, 
    CITY AS City,
    STATE AS State,
    COUNTRY AS Country,
    ZIP_CODE As ZipCode,
    IS_CURRENT AS IsCurrent,
    LOAD_TIMESTAMP AS LoadTimeStamp
FROM
    {{source('silver','customer_slvr')}}
