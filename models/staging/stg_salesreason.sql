with
    salesreason as (
    select 
        cast(salesreasonid as numeric) as salesreason_pk
        , cast(name as string) as name_reason
        , reasontype 
        , cast(modifieddate as date) as modifieddate
    from {{ source('desafio_lh_marcelo', 'salesreason') }}
    )

select *
from salesreason
