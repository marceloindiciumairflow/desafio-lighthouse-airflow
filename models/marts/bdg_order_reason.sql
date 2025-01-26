with 
    fct_sales as (
        select 
            order_pk 
            , orderdetail_pk
        from {{ ref('fct_sales') }}
    )

    , salesreason as (
        select 
            salesreason_pk
        from {{ ref('stg_salesreason') }}
    )

    , salesorderheader as (
        select 
            order_fk
            , salesreason_fk 
        from {{ ref('stg_salesorderheadersalesreason') }}
    )

    , aggregated_salesreason as (
        select 
            order_fk as order_sales_fk
            , min(salesreason_fk) as salesreason_sales_fk
        from salesorderheader
        group by order_fk
    )
 
    , bdg_order_salesreason as (
        select distinct
            md5(concat(order_sales_fk, salesreason_sales_fk)) as order_salesreason_sk
            , order_sales_fk
            , salesreason_sales_fk
        from aggregated_salesreason
        left join fct_sales
            on aggregated_salesreason.order_sales_fk = fct_sales.order_pk
    )

select *
from bdg_order_salesreason
