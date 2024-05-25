
{{ config(materialized='external', location='/home/cyril/Documents/vscode project/dbt_for_dvf/output/dvf_dbt_model.parquet', format='parquet')}}

select *
from {{ ref('top_delta_valeur_fonciere') }}

