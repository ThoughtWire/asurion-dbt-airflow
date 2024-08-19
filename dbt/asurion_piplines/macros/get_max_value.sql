{% macro get_max_value(table_name, col_name, offset=0) %}
    {% set query %}

        SELECT {{ col_name }}  AT TIME ZONE 'UTC' + INTERVAL '{{ offset }} hour'
        FROM {{ table_name }}
        ORDER BY {{ col_name }} DESC 
        LIMIT 1

    {% endset %}

    {% set results = run_query(query) %}

    {% if results and results.rows %}
        {{ return("'" ~ results.rows[0][0] ~ "'") }}
    {% else %}
        {{ return("'1990-01-01'") }}
    {% endif %}
    
{% endmacro %}
