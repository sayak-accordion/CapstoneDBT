{{config(
    materialized= 'view'
)}}

WITH ProductSold AS (
    SELECT
        fs.productkey,
        SUM(quantitysold) AS QuantitySold
    FROM
        gold.fact_sales fs
    GROUP BY
        fs.productkey
)
SELECT 
    ps.productKey,
    dp.productName,
    ps.QuantitySold
FROM
    PRODUCTSOLD ps JOIN gold.dim_product dp
    ON ps.productKey = dp.productkey
ORDER BY
    ps.QuantitySold DESC

