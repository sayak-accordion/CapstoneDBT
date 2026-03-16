{{config(
    materialized= 'view'
)}}

SELECT 
    REGION,
    SUM(totalsalesamount) AS TotalSales
FROM
    gold.fact_sales
GROUP BY
    region
