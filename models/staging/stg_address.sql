with
    address as (
        select
            cast(addressid as numeric) as address_pk
            , cast(stateprovinceid as numeric) as stateprovince_fk
            , addressline1
            , addressline2
            , city
            , cast(modifieddate as date) as modifieddate
            , postalcode
            , rowguid
            , spatiallocation
        from {{ source("desafio_lh_marcelo", "address") }}
    )

select *
from address
