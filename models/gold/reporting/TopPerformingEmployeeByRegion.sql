{{config(
    materialized='view'
)}}

SELECT 
    role,
    SUM(fs.TOTALSALESAMOUNT) AS SaleContribution
FROM
    gold.fact_sales fs JOIN gold.dim_employee de
    ON fs.employeekey = de.employeekey
GROUP BY 
    role
ORDER BY
    SaleContribution DESC
