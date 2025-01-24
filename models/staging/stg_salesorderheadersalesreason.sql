with
    salesorderheadersalesreason as (
    select 
        md5(concat(cast(salesorderid as numeric), cast(salesreasonid as numeric))) as salesorderheadersalesreason_sk
        , cast(salesorderid as numeric) as order_fk
        , cast(salesreasonid as numeric) as salesreason_fk
        , cast(modifieddate as date) as modifieddate
    from {{ source('desafio_lh_marcelo', 'salesorderheadersalesreason') }} 
    )

select *
from salesorderheadersalesreason

