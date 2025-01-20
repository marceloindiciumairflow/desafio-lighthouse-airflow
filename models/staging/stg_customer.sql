with
    customer as (
    select
        cast (customerid as numeric) as customer_pk
        , cast(personid as numeric) as person_fk
        , storeid
        , territoryid
        , cast(modifieddate as date) as modifieddate
        , cast(rowguid as string) as rowguid_customer
    from {{ source('desafio_lh_marcelo', 'customer') }}
    )

select *
from customer
