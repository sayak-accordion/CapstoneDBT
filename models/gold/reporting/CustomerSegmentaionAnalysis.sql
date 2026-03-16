{{config(
    materialized = 'view'
)}}

SELECT 
    dc.customersegment,
    COUNT(1) AS TotalOrders,
    SUM(totalsalesamount) AS TotalSales,
    CAST(
        TotalSales/TotalOrders AS DECIMAL(10,2)
    ) AS AvgOrderValue
FROM
    gold.fact_sales fs JOIN gold.dim_customer dc
    ON fs.customerkey= dc.customerkey
GROUP BY
    dc.customersegment
