{{config(
    materialized='view'
)}}

WITH BuildRepeatPurchaseFlag AS (
    SELECT 
        fs.customerKey,
        fs.saleskey,
        ROW_NUMBER()OVER(
            PARTITION BY customerkey, productkey
            ORDER BY dd.fulldate ASC
        ) AS rn
    FROM
        gold.fact_sales fs JOIN gold.dim_date dd
        ON fs.datekey = dd.datekey
)
SELECT
    customerKey,
    SUM(
        CASE
            WHEN rn>1 THEN 1
            ELSE 0
        END
    ) AS RepeatPurchases,
    SUM(
        CASE
            WHEN rn=1 THEN 1
            ELSE 0
        END
    ) AS FirstPurchases,
    RepeatPurchases/FirstPurchases
    AS RepeatPurchaseRate
FROM
    BUILDREPEATPURCHASEFLAG
GROUP BY
    customerKey
ORDER BY RepeatPurchaseRate DESC
