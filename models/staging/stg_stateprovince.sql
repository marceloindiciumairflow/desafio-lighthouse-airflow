with 
    stateprovince as (
        select 
            cast(stateprovinceid as numeric) as stateprovince_pk
            , cast(territoryid as numeric) as territory_fk
            , cast(countryregioncode as string) as countryregion_fk
            , stateprovincecode
            , isonlystateprovinceflag
            , cast(name as string) as name_state
            , cast(rowguid as string) as rowguid_state
            , cast(modifieddate as date) as modifieddate
        from {{ source('desafio_lh_marcelo', 'stateprovince') }}
    )

select *
from stateprovince
