SELECT
    UUID_STRING(
        'efc3a943-a363-4dc4-847e-06b8b0cfddc5',
        CAMPAIGN_ID 
    ) AS CampaignKey,
    CAMPAIGN_ID AS CampaignID, 
    TARGET_TYPE AS TargetType,
    TARGET_AGE AS TargetAge,
    TARGET_LOCATION AS TargetLocation,
    START_DATE AS StartDate,
    END_DATE AS EndDate,
    CAMPAIGN_DURATION_DAYS AS Duration,
    BUDGET AS Budget,
    ROI_CALCULATED AS ROI
FROM
    {{source('silver','campaign_slvr')}}
