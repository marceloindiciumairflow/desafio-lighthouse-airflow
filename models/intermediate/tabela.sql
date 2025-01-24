with
    fct_sales as (
        select 
            order_pk as order_sales_fk
            , orderdetail_pk
            , product_pk
            , subtotal
            , totaldue
            , customer_fk
            , shipaddress_fk
            , unitprice
            , orderqty
            , valor_bruto
            , gross_value
            , orderdate
            , order_month
            , order_year
            , businessentity_fk
            , creditcard_pk
        from {{ ref('fct_sales') }}
    )

    , address as (
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

    , territory as (
        select
            territory_name
            , territory_pk
        from {{ ref('stg_salesterritory') }}
    )

    , salesorder as (
        select 
            order_pk
            , shipaddress_fk
        from {{ ref('stg_salesorderheader') }}
    )

    , join_sales_address as (
        select distinct
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
        select distinct
            join_sales_address.order_pk
            , join_sales_address.city
            , state.name_state
            , state.countryregion_fk
            , state.territory_fk
        from join_sales_address
        left join state
            on join_sales_address.stateprovince_fk = state.stateprovince_pk
    )

    , join_country_territory as (
        select distinct
            join_state.order_pk
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

    , employee as (
        select 
            cast(employee_pk as string) as employee_pk
            , nationalidnumber
            , fullname as employee_name
            , hiredate
            , birthdate
        from {{ ref('stg_employee') }}
        left join {{ ref('stg_person') }}
            on cast(employee_pk as string) = cast(business_fk as string)
    )

    , enriched_sales as (
        select
            fct_sales.order_sales_fk
            , fct_sales.orderdetail_pk
            , fct_sales.product_pk
            , fct_sales.subtotal
            , fct_sales.totaldue
            , fct_sales.customer_fk
            , fct_sales.shipaddress_fk
            , fct_sales.unitprice
            , fct_sales.orderqty
            , fct_sales.valor_bruto
            , fct_sales.gross_value
            , fct_sales.orderdate
            , fct_sales.order_month
            , fct_sales.order_year
            , fct_sales.businessentity_fk
            , fct_sales.creditcard_pk
            , join_country_territory.city
            , join_country_territory.name_state
            , join_country_territory.country_name
            , join_country_territory.territory_name
            , employee.employee_name
            , employee.nationalidnumber
            , employee.hiredate
            , employee.birthdate
        from fct_sales
        left join join_country_territory
            on fct_sales.shipaddress_fk = join_country_territory.order_pk
        left join employee
            on fct_sales.businessentity_fk = employee.employee_pk
    )

select
    enriched_sales.order_sales_fk
    , enriched_sales.orderdetail_pk
    , enriched_sales.product_pk
    , enriched_sales.subtotal
    , enriched_sales.totaldue
    , enriched_sales.customer_fk
    , enriched_sales.shipaddress_fk
    , enriched_sales.unitprice
    , enriched_sales.orderqty
    , enriched_sales.valor_bruto
    , enriched_sales.gross_value
    , enriched_sales.orderdate
    , enriched_sales.order_month
    , enriched_sales.order_year
    , enriched_sales.businessentity_fk
    , enriched_sales.creditcard_pk
    , enriched_sales.city
    , enriched_sales.name_state
    , enriched_sales.country_name
    , enriched_sales.territory_name
    , enriched_sales.employee_name
    , enriched_sales.nationalidnumber
    , enriched_sales.hiredate
    , enriched_sales.birthdate
from enriched_sales
order by city
