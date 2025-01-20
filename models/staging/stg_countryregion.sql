with 
    countryregion as (
        select 
            cast(countryregioncode as string) as countryregion_pk
            , cast(name as string) as country_name  
            , cast(modifieddate as date) as modifieddate      
        from {{ source('desafio_lh_marcelo', 'countryregion') }}
    )

select *
from countryregion
