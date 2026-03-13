{{ config(
    materialized='incremental',
    unique_key='supplier_id'
) }}


SELECT
    f.value:supplier_id::STRING AS supplier_id,
    f.value:supplier_name::STRING AS supplier_name,
    f.value:supplier_type::STRING AS supplier_type,

    f.value:tax_id::STRING AS tax_id,
    f.value:website::STRING AS website,
    f.value:credit_rating::STRING AS credit_rating,
    f.value:payment_terms::STRING AS payment_terms,
    f.value:preferred_carrier::STRING AS preferred_carrier,

    f.value:is_active::BOOLEAN AS is_active,

    f.value:year_established::INT AS year_established,
    f.value:lead_time_days::INT AS lead_time_days,
    f.value:minimum_order_quantity::INT AS minimum_order_quantity,

    TRY_TO_DATE(f.value:last_order_date::STRING) AS last_order_date,
    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date,

    f.value:contact_information:contact_person::STRING AS contact_person,
    f.value:contact_information:email::STRING AS contact_email,
    f.value:contact_information:phone::STRING AS contact_phone,
    f.value:contact_information:address::STRING AS contact_address,

    f.value:contract_details:contract_id::STRING AS contract_id,
    TRY_TO_DATE(f.value:contract_details:start_date::STRING) AS contract_start_date,
    TRY_TO_DATE(f.value:contract_details:end_date::STRING) AS contract_end_date,
    f.value:contract_details:renewal_option::BOOLEAN AS contract_renewal_option,
    f.value:contract_details:exclusivity::BOOLEAN AS contract_exclusivity,

    f.value:performance_metrics:average_delay_days::NUMBER(6,2) AS average_delay_days,
    f.value:performance_metrics:defect_rate::NUMBER(6,2) AS defect_rate,
    f.value:performance_metrics:on_time_delivery_rate::NUMBER(6,2) AS on_time_delivery_rate,
    f.value:performance_metrics:returns_percentage::NUMBER(6,2) AS returns_percentage,
    f.value:performance_metrics:response_time_hours::INT AS response_time_hours,
    f.value:performance_metrics:quality_rating::STRING AS quality_rating,

    f.value:categories_supplied AS categories_supplied,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{ source('raw','ext_supplier') }},
LATERAL FLATTEN(input => raw_data:suppliers_data) f

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}