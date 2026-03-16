SELECT
    UUID_STRING(
        'efc3a943-a363-4dc4-847e-06b8b0cfddc5',
        PRODUCT_ID
    ) AS ProductKey,
    PRODUCT_ID AS ProductID, 
    PRODUCT_NAME AS ProductName,
    CATEGORY AS Category, 
    SUBCATEGORY AS SubCategory,
    BRAND AS Brand, 
    COLOR AS Color,
    SIZE AS Size,
    COST_PRICE AS CostPrice, 
    UNIT_PRICE AS UnitPrice,
    SUPPLIER_ID AS SupplierID
FROM
    {{source('silver','product_slvr')}}
