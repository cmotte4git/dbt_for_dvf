with source as (
      select * from  read_PARQUET('/home/cyril/Documents/vscode project/dbt_for_dvf/data/dvf_full.parquet')
),
dvf_full as (
    select
        *

    from source
)
select * from dvf_full
  