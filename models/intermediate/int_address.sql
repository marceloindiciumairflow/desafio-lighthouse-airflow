with 
    address as (
        select 
            address_pk
            , stateprovince_fk
            , city
        from {{ ref('stg_address') }}
    )

    , state as (
        select 
            name_state
            , countryregion_fk
            , territory_fk
            , stateprovince_pk
        from {{ ref('stg_stateprovince') }}
    )

    , country as (
        select 
            country_name
            , countryregion_pk
        from {{ ref('stg_countryregion') }}
    )

    , salesorder as (
        select 
            order_pk
            , shipaddress_fk
        from {{ ref('stg_salesorderheader') }}
    )

    , territory as (
        select
            territory_name
            , territory_pk
        from {{ ref('stg_salesterritory') }}
    )

    , join_sales_address as (
        select
            salesorder.order_pk 
            , salesorder.shipaddress_fk
            , address.address_pk
            , address.stateprovince_fk
            , address.city
        from salesorder
        left join address
                on salesorder.shipaddress_fk = address.address_pk
    )

    , join_state as (
        select
            join_sales_address.order_pk 
            , join_sales_address.address_pk
            , join_sales_address.city
            , state.name_state
            , state.countryregion_fk
            , state.territory_fk
        from join_sales_address
        left join state 
            on join_sales_address.stateprovince_fk = state.stateprovince_pk
    )

    , join_country_territory as (
        select
            join_state.address_pk
            , join_state.city
            , join_state.name_state
            , country.country_name
            , territory.territory_name
        from join_state
        left join country 
            on join_state.countryregion_fk = country.countryregion_pk
        left join territory
            on join_state.territory_fk = territory.territory_pk
    )

select *
from join_country_territory
