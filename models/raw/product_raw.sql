{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

SELECT
    f.value:product_id::STRING AS product_id,
    f.value:name::STRING AS product_name,
    f.value:brand::STRING AS brand,

    f.value:category::STRING AS category,
    f.value:subcategory::STRING AS subcategory,
    f.value:product_line::STRING AS product_line,

    f.value:color::STRING AS color,
    f.value:size::STRING AS size,

    f.value:short_description::STRING AS short_description,
    f.value:technical_specs::STRING AS technical_specs,

    f.value:supplier_id::STRING AS supplier_id,

    f.value:dimensions::STRING AS dimensions,
    f.value:weight::STRING AS weight,
    f.value:warranty_period::STRING AS warranty_period,

    f.value:is_featured::BOOLEAN AS is_featured,

    TRY_TO_DATE(f.value:launch_date::STRING) AS launch_date,
    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date,

    f.value:stock_quantity::INT AS stock_quantity,
    f.value:reorder_level::INT AS reorder_level,

    f.value:cost_price::NUMBER(12,2) AS cost_price,
    f.value:unit_price::NUMBER(12,2) AS unit_price,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{ source('raw','ext_product') }},
LATERAL FLATTEN(input => raw_data:products_data) f

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}