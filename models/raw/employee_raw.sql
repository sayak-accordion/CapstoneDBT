{{ config(
    materialized='incremental',
    unique_key='employee_id'
) }}


SELECT
    f.value:employee_id::STRING AS employee_id,

    f.value:first_name::STRING AS first_name,
    f.value:last_name::STRING AS last_name,
    f.value:email::STRING AS email,
    f.value:phone::STRING AS phone,

    f.value:role::STRING AS role,
    f.value:department::STRING AS department,
    f.value:education::STRING AS education,
    f.value:employment_status::STRING AS employment_status,

    f.value:manager_id::STRING AS manager_id,
    f.value:work_location::STRING AS work_location,

    TRY_TO_DATE(f.value:date_of_birth::STRING) AS date_of_birth,
    TRY_TO_DATE(f.value:hire_date::STRING) AS hire_date,
    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date,

    f.value:salary::NUMBER(12,2) AS salary,
    f.value:current_sales::NUMBER(12,2) AS current_sales,
    f.value:sales_target::NUMBER(12,2) AS sales_target,
    f.value:performance_rating::NUMBER(3,1) AS performance_rating,

    f.value:address:street::STRING AS address_street,
    f.value:address:city::STRING AS address_city,
    f.value:address:state::STRING AS address_state,
    f.value:address:zip_code::STRING AS address_zip_code,

    f.value:certifications AS certifications,

    MD5(TO_VARCHAR(f.value)) AS record_hash,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS load_timestamp,

    filename,
    file_row_number

FROM {{ source('raw','ext_employee') }},
LATERAL FLATTEN(input => raw_data:employees_data) f

{% if is_incremental() %}
WHERE filename NOT IN (
    SELECT DISTINCT filename FROM {{ this }}
)
{% endif %}