{% macro create_external_tables() %}

{% set metadata_query %}
    SELECT 
        tablename,
        schema,
        stage,
        folder,
        fileformat
    FROM raw.ingestionmetadata
{% endset %}

{% set results = run_query(metadata_query) %}

{% if execute %}

    {% for row in results.rows %}

        {% set table_name = row[0] %}
        {% set schema_name = row[1] %}
        {% set stage_name = row[2] %}
        {% set folder_name = row[3] %}
        {% set file_format = row[4] %}

        {% set create_table_sql %}

        CREATE OR REPLACE EXTERNAL TABLE {{ schema_name }}.ext_{{ table_name }}
        (
            raw_data VARIANT AS (value),
            filename STRING AS METADATA$FILENAME,
            file_row_number NUMBER AS METADATA$FILE_ROW_NUMBER
        )
        WITH LOCATION = @{{ stage_name }}/Capstone_Project_Data/{{ folder_name }}/
        FILE_FORMAT = (FORMAT_NAME = {{ file_format }})
        AUTO_REFRESH = FALSE;

        {% endset %}

        {{ log("Creating external table for " ~ table_name, info=True) }}

        {% do run_query(create_table_sql) %}

    {% endfor %}

{% endif %}

{% endmacro %}