{{ config(materialized = 'materialized_view', tags = ['zones']) }} 


with zl as 

(select distinct * from {{
    source('raw_data_prod', 'dti_zl')

}})

select * from zl