{{config(
    materialized = 'view'
)}}

SELECT 
    dp.category AS ProductCategory,
    dp.subcategory AS SubCategory,
    SUM(fs.totalsalesamount) AS TotalSales
FROM
    gold.fact_sales fs JOIN gold.dim_product dp
    ON fs.productkey = dp.productkey
GROUP BY
    dp.category, dp.subcategory

