
SELECT
    CAMPAIGN_ID, 
    UPPER(CAMPAIGN_NAME) AS campaign_name,
    UPPER(CAMPAIGN_TYPE) AS campaign_type,
    UPPER(CHANNEL) AS channel,
    UPPER(DESCRIPTION) AS description,
    UPPER(TARGET_AUDIENCE) AS target_audience,
    UPPER(
        SUBSTR(target_audience,
                0,
                CHARINDEX(',',target_audience)-1)
    ) AS target_type,
    SUBSTR(target_audience,
        CHARINDEX(',',target_audience)+2,
        CHARINDEX(',',
                target_audience,
                CHARINDEX(',',target_audience)+2)-CHARINDEX(',',target_audience)-2
    ) AS target_age,
    UPPER(
        SUBSTR(target_audience,
            CHARINDEX(',',
                    target_audience,
                    CHARINDEX(',',target_audience)+2)+2
        )
    ) AS target_location,
    START_DATE,
    END_DATE,
    DATEDIFF(day,START_DATE,END_DATE) AS campaign_duration_days,
    LAST_MODIFIED_DATE,
    BUDGET,
    TOTAL_COST,
    TOTAL_REVENUE,
    ROI_CALCULATION AS ROI_RAW,
    CAST((TOTAL_REVENUE-TOTAL_COST)/TOTAL_COST AS DECIMAL(10,2)) AS ROI_CALCULATED,
    CASE 
        WHEN ROI_RAW=ROI_CALCULATED THEN TRUE
        ELSE FALSE
    END
    AS is_roi_correct,
    -----------------------
    RECORD_HASH,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    IS_CURRENT,
    LOAD_TIMESTAMP,
    FILENAME,
    FILE_ROW_NUMBER
FROM
    {{source('bronze', 'campaign_brnz')}}