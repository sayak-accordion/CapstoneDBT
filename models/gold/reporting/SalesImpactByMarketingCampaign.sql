{{config(
    materialized='view'
)}}

SELECT * FROM gold.fact_marketingperformance
ORDER BY TOTALSALESINFLUENCE DESC