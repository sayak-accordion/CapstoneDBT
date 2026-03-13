{{ config(
    materialized='incremental',
    unique_key='store_id'
) }}


SELECT
    f.value:store_id::STRING AS store_id,
    f.value:store_name::STRING AS store_name,
    f.value:store_type::STRING AS store_type,
    f.value:region::STRING AS region,

    f.value:manager_id::STRING AS manager_id,
    f.value:email::STRING AS email,
    f.value:phone_number::STRING AS phone_number,

    f.value:is_active::BOOLEAN AS is_active,

    f.value:employee_count::INT AS employee_count,
    f.value:size_sq_ft::INT AS size_sq_ft,

    f.value:current_sales::NUMBER(18,2) AS current_sales,
    f.value:sales_target::NUMBER(18,2) AS sales_target,
    f.value:monthly_rent::NUMBER(12,2) AS monthly_rent,

    TRY_TO_DATE(f.value:opening_date::STRING) AS opening_date,
    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date,

    f.value:address:street::STRING AS address_street,
    f.value:address:city::STRING AS address_city,
    f.value:address:state::STRING AS address_state,
    f.value:address:country::STRING AS address_country,
    f.value:address:zip_code::STRING AS address_zip_code,

    f.value:operating_hours:weekdays::STRING AS operating_hours_weekdays,
    f.value:operating_hours:weekends::STRING AS operating_hours_weekends,
    f.value:operating_hours:holidays::STRING AS operating_hours_holidays,

    f.value:services AS services,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{ source('raw','ext_store') }},
LATERAL FLATTEN(input => raw_data:stores_data) f

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}