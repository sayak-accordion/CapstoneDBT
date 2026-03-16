{{config(
    materialized='view'
)}}

SELECT
    campaign_type,
    CAST(
        AVG(ROI_CALCULATED) AS DECIMAL(10,2)
    ) AS AvgROI
FROM
    silver.campaign_slvr
GROUP BY
    campaign_type
ORDER BY AvgROI DESC
