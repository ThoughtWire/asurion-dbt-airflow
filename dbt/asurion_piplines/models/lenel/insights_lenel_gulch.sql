-- materiazlied view for insights entries which joins agg table with dto_headcount

{{ config(materialized = 'incremental', 
  unique_key = ['ssno', '"EmpId"', '"EventDateTimeEastern"'],   
  incremental_strategy = 'delete+insert', 
  indexes = [{'columns': ['"EventDateTimeEastern"'],
    'type': 'btree' } ], 
    tags = ['lenel_gulch'],
    sort = '"EventDateTimeEastern"') }}

{% if is_incremental() %}
{% set max_time = get_max_value('insights_lenel_gulch', '"EventDateTimeEastern"') %}
{% endif %}


{% set bom = beginning_of_month('current_timestamp') %}


with updated_entries as (

select *

from {{ ref('insights_lenel_global') }}

where bldgcode = 'NGHB' 

{% if is_incremental() %}

and "EventDateTimeEastern" >= date_trunc('day', {{ max_time }}::timestamp)

{% endif %}

)


select * from updated_entries

