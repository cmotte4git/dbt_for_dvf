with source as (
      select * from  {{source ('external_source', 'dvf_full')}}
),
dvf_full as (
    select
        *

    from source
)
select * from dvf_full
  
  