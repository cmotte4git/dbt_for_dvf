
{{ config(materialized='external', location='output/dvf_dbt_model.parquet', format='parquet')}}

select *
from {{ ref('top_delta_valeur_fonciere') }}

