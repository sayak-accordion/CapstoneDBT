SELECT
    UUID_STRING(
        'efc3a943-a363-4dc4-847e-06b8b0cfddc5',
        STORE_ID
    ) AS StoreKey,
    STORE_ID AS StoreID, 
    STORE_NAME  AS StoreName,
    STORE_TYPE AS StoreType,
    SIZE_CATEGORY AS SizeCategory,
    OPENING_DATE AS OpeningDate,
    REGION AS Region,
    ADDRESS_STREET AS AddressStreet,
    ADDRESS_CITY AS AddressCity, 
    ADDRESS_STATE AS AddressState,
    ADDRESS_COUNTRY AS AddressCountry,
    ADDRESS_ZIP_CODE AS AddressZipCode
FROM
    {{source('silver','store_slvr')}}
