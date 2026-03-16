
SELECT 
    UUID_STRING(
        'efc3a943-a363-4dc4-847e-06b8b0cfddc5',
        SUPPLIER_ID 
    ) AS SupplierKey,
    SUPPLIER_ID AS SUpplierID, 
    SUPPLIER_NAME AS SupplierName, 
    SUPPLIER_TYPE AS SupplierType,
    PAYMENT_TERMS AS PaymentTerms,
    CONTACT_PERSON AS ContactPerson,
    CONTACT_EMAIL AS ContactEmail,
    CONTACT_PHONE AS ContactPhone,
    CONTACT_ADDRESS AS ContactAddress
FROM
    {{source('silver','supplier_slvr')}}
