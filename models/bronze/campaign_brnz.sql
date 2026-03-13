-- BRONZE TRANSFORMATIONS (STORED IN BRONZE SCHEMA)

WITH numberedCampaign AS(
    SELECT 
        *,
        ROW_NUMBER()OVER (
            PARTITION BY campaign_id
            ORDER BY campaign_id
        ) AS rn
    FROM {{source('raw','campaign_raw')}}
)
SELECT 
    CAMPAIGN_ID, 
    CAMPAIGN_NAME, 
    CAMPAIGN_TYPE, 
    CHANNEL, 
    DESCRIPTION, 
    TARGET_AUDIENCE, 
    START_DATE, 
    END_DATE, 
    LAST_MODIFIED_DATE, 
    ROI_CALCULATION, 
    BUDGET, 
    TOTAL_COST, 
    TOTAL_REVENUE, 
    RECORD_HASH, 
    EFFECTIVE_FROM, 
    EFFECTIVE_TO, 
    IS_CURRENT, 
    LOAD_TIMESTAMP, 
    FILENAME, 
    FILE_ROW_NUMBER
FROM
    numberedcampaign
WHERE
    rn = 1
