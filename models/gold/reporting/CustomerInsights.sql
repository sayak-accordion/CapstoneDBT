{{config(
    materialized='view'
)}}

SELECT
    CustomerKey,
    SUM(TOTALSALESAMOUNT) AS LifeTimeValue
FROM 
    gold.fact_sales
GROUP BY
    CUSTOMERKEY
