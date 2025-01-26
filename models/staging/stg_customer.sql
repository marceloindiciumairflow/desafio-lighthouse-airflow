with
    customer as (
    select
        cast (customerid as string) as customer_pk
        , cast(personid as string) as person_fk
        , storeid
        , territoryid
        , cast(modifieddate as date) as modifieddate
        , cast(rowguid as string) as rowguid_customer
    from {{ source('desafio_lh_marcelo', 'customer') }}
    )

select *
from customer
