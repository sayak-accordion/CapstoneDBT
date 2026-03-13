{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id']
) }}


SELECT
    f.value:order_id::STRING AS order_id,

    item.value:product_id::STRING AS product_id,
    item.value:quantity::INT AS quantity,

    item.value:unit_price::NUMBER(12,2) AS unit_price,
    item.value:cost_price::NUMBER(12,2) AS cost_price,
    item.value:discount_amount::NUMBER(12,2) AS discount_amount,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{ source('raw','ext_orders') }},
LATERAL FLATTEN(input => raw_data:orders_data) f,
LATERAL FLATTEN(input => f.value:order_items) item

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}