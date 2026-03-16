{{config(
    materialized='view'
)}}

SELECT
    tenure_yrs AS tenure,
    CAST(
        AVG(performance_rating) AS DECIMAL(10,2)
    )AS AvgRating,
    CAST(
        AVG(TARGET_ACHIEVEMENT_PERCENTAGE) AS DECIMAL(10,2)
    ) AS Avg_TargetAcheivementPercentage,
    CAST(
        AVG(ORDERS_PROCESSED/tenure) AS DECIMAL(10,2)
    ) AS Avg_OrdersProcessedPerYear,
    CAST(
        AVG(TOTAL_SALES_AMOUNT/tenure) AS DECIMAL(10,2)
    )AS Avg_TotalSalesPerYear
FROM
    silver.employee_slvr
GROUP BY 
    tenure_yrs
ORDER BY tenure DESC
