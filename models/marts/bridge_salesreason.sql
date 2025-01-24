with
    salesreason_data as (
        select
            reason_sk
            , salesreason_pk
            , name_reason
            , reasontype
        from {{ ref('dim_salesreason') }}
    )
    , sales_data as (
        select
            sales_sk
            , salesreason_fk
        from {{ ref('fct_sales') }}
    )
    , bridge_salesreason as (
        select
            md5(concat(sales_sk, reason_sk)) as bridge_sk
            , sales_sk
            , reason_sk
            , row_number() over (partition by sales_sk, salesreason_fk order by reason_sk) as bridge_row_number
            , salesreason_pk
            , name_reason
            , reasontype
        from sales_data
        inner join salesreason_data
            on sales_data.salesreason_fk = salesreason_data.salesreason_pk
    )

select *
from bridge_salesreason
