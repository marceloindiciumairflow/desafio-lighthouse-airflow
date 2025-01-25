with
    salesreason_data as (
        select
            reason_sk
            , salesreason_pk
        from {{ ref('dim_salesreason') }}
        where salesreason_pk between 0 and 10 -- Filtra os valores de salesreason_pk
    )
    , representative_orders as (
        select
            salesreason_fk
            , min(order_pk) as representative_order_pk -- Seleciona um valor representativo de order_pk
        from {{ ref('fct_sales') }}
        where salesreason_fk between 0 and 10 -- Filtra os valores de salesreason_fk
        group by salesreason_fk
    )
    , unique_salesreason as (
        select
            salesreason_pk
            , reason_sk
            , coalesce(representative_order_pk, 0) as order_pk -- Garante que order_pk não seja nulo
        from salesreason_data
        left join representative_orders
            on salesreason_data.salesreason_pk = representative_orders.salesreason_fk
    )
    , bridge_salesreason as (
        select
            md5(concat(cast(salesreason_pk as string), cast(order_pk as string))) as bridge_sk
            , salesreason_pk
            , order_pk
            , reason_sk
        from unique_salesreason
    )

select salesreason_pk
    , bridge_sk
    , reason_sk
from bridge_salesreason
group by salesreason_pk

