with
    salesreason_header as (
        select
            salesorderheadersalesreason_sk
            , salesreason_fk
            , order_fk
        from {{ ref('stg_salesorderheadersalesreason') }}
    )
    , salesreason as (
        select 
            salesreason_pk
            , name_reason
            , reasontype 
            , modifieddate
        from {{ ref('stg_salesreason') }}
    )
    , dim_salesreason as (
        select
            md5(concat(salesreason_pk, order_fk)) as reason_sk 
            , salesreason_pk
            , order_fk
            , name_reason
            , reasontype 
            , modifieddate
            , salesorderheadersalesreason_sk
        from salesreason_header
        left join salesreason
            on salesreason_header.salesreason_fk = salesreason.salesreason_pk
        
        union all
        
        select
            md5(concat('0', '0', 'no reason')) as reason_sk
            , 0 as salesreason_pk
            , 0 as order_fk
            , 'No reason' as name_reason
            , 'NA' as reasontype
            , null as modifieddate
            , md5(concat('0', 'unique_default')) as salesorderheadersalesreason_sk 
    )

select *
from dim_salesreason
