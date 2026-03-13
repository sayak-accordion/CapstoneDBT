{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}


SELECT
    f.value:customer_id::STRING AS customer_id,

    f.value:first_name::STRING AS first_name,
    f.value:last_name::STRING AS last_name,
    f.value:email::STRING AS email,
    f.value:phone::STRING AS phone,

    f.value:occupation::STRING AS occupation,
    f.value:income_bracket::STRING AS income_bracket,
    f.value:loyalty_tier::STRING AS loyalty_tier,

    f.value:preferred_communication::STRING AS preferred_communication,
    f.value:preferred_payment_method::STRING AS preferred_payment_method,

    f.value:marketing_opt_in::BOOLEAN AS marketing_opt_in,

    TRY_TO_DATE(f.value:birth_date::STRING) AS birth_date,
    TRY_TO_DATE(f.value:registration_date::STRING) AS registration_date,
    TRY_TO_DATE(f.value:last_purchase_date::STRING) AS last_purchase_date,
    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date,

    f.value:total_purchases::INT AS total_purchases,
    f.value:total_spend::NUMBER(10,2) AS total_spend,

    -- Nested address fields
    f.value:address:street::STRING AS street,
    f.value:address:city::STRING AS city,
    f.value:address:state::STRING AS state,
    f.value:address:country::STRING AS country,
    f.value:address:zip_code::STRING AS zip_code,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{source('raw', 'ext_customer')}},
LATERAL FLATTEN(input => raw_data:customers_data) f

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}
