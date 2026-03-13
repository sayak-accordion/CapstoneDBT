{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}


SELECT
    f.value:order_id::STRING AS order_id,
    f.value:customer_id::STRING AS customer_id,
    f.value:employee_id::STRING AS employee_id,
    f.value:campaign_id::STRING AS campaign_id,
    f.value:store_id::STRING AS store_id,

    f.value:order_source::STRING AS order_source,
    f.value:order_status::STRING AS order_status,
    f.value:payment_method::STRING AS payment_method,
    f.value:shipping_method::STRING AS shipping_method,

    TRY_TO_TIMESTAMP(f.value:created_at::STRING) AS created_at,
    TRY_TO_TIMESTAMP(f.value:order_date::STRING) AS order_date,
    TRY_TO_TIMESTAMP(f.value:shipping_date::STRING) AS shipping_date,
    TRY_TO_TIMESTAMP(f.value:delivery_date::STRING) AS delivery_date,
    TRY_TO_TIMESTAMP(f.value:estimated_delivery_date::STRING) AS estimated_delivery_date,

    f.value:shipping_cost::NUMBER(12,2) AS shipping_cost,
    f.value:discount_amount::NUMBER(12,2) AS discount_amount,
    f.value:tax_amount::NUMBER(12,2) AS tax_amount,
    f.value:total_amount::NUMBER(12,2) AS total_amount,

    f.value:billing_address:street::STRING AS billing_street,
    f.value:billing_address:city::STRING AS billing_city,
    f.value:billing_address:state::STRING AS billing_state,
    f.value:billing_address:zip_code::STRING AS billing_zip_code,

    f.value:shipping_address:street::STRING AS shipping_street,
    f.value:shipping_address:city::STRING AS shipping_city,
    f.value:shipping_address:state::STRING AS shipping_state,
    f.value:shipping_address:zip_code::STRING AS shipping_zip_code,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{ source('raw','ext_orders') }},
LATERAL FLATTEN(input => raw_data:orders_data) f

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}