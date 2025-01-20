with
    salesterritory as (
    select 
        cast(territoryid as numeric) as  territory_pk
        , cast(name as string) as territory_name
        , countryregioncode 
        , "group" as group_
        , salesytd 
        , saleslastyear 
        , costytd 
        , costlastyear 
        , rowguid 
        , cast(modifieddate as date) as modifieddate
    from {{ source('desafio_lh_marcelo', 'salesterritory') }} 
    )

select *
from salesterritory
