{{config(
    materialized= 'incremental',
    unique_key= 'campaign_id'
)}}

SELECT
    f.value:campaign_id::STRING AS campaign_id,
    f.value:campaign_name::STRING AS campaign_name,
    f.value:campaign_type::STRING AS campaign_type,
    f.value:channel::STRING AS channel,
    f.value:description::STRING AS description,
    f.value:target_audience::STRING AS target_audience,

    TRY_TO_TIMESTAMP(f.value:start_date::STRING) AS start_date,
    TRY_TO_TIMESTAMP(f.value:end_date::STRING) AS end_date,
    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date,

    TRY_TO_NUMBER(f.value:roi_calculation::STRING) AS roi_calculation,

    TRY_TO_NUMBER(REGEXP_REPLACE(f.value:budget::STRING,'[$,]','')) AS budget,
    TRY_TO_NUMBER(REGEXP_REPLACE(f.value:total_cost::STRING,'[$,]','')) AS total_cost,
    TRY_TO_NUMBER(REGEXP_REPLACE(f.value:total_revenue::STRING,'[$,]','')) AS total_revenue,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{source('raw', 'ext_campaign')}},
LATERAL FLATTEN(input => raw_data:campaigns_data) f

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}