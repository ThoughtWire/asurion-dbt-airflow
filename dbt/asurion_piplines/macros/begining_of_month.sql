{% macro beginning_of_month(timestamp) %}

{% set first_of_month_query %}

    select date_trunc('month', {{ timestamp }})
    
{% endset %}

{% set result = run_query(first_of_month_query) %}

{% if execute %}

    {% if result and result.columns is iterable and result.columns[0] %}

        {{ return(result.rows[0][0]) }}

    {% else %}

        {{ return(none) }}

    {% endif %}

{% endif %}

{% endmacro %}
