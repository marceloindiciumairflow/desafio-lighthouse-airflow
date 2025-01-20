with
    salesreason as(
        select 
            salesreason_pk
            , name_reason
            , reasontype
        from {{ ref('stg_salesreason') }}
    )

    , orderreason as (
        select 
            salesreason_fk
            , order_fk
        from {{ ref('stg_salesorderheadersalesreason') }}
    )

    , salesorder as (
        select
            order_pk 
            , customer_fk
        from {{ ref('int_order_header') }}
    )

    , join_reason as (
        select 
            salesreason.salesreason_pk
	        , salesreason.name_reason
	        , salesreason.reasontype
            , orderreason.order_fk 
            from orderreason 
            left join salesreason
                    on orderreason.salesreason_fk = salesreason.salesreason_pk
        )

    , join_reason_header as(
        select
            join_reason.salesreason_pk
            , join_reason.order_fk 
            , salesorder.order_pk 
            , salesorder.customer_fk
            , join_reason.name_reason
	        , join_reason.reasontype
        from salesorder
        left join join_reason 
                on salesorder.order_pk = join_reason.order_fk 
    )

    , coalesce_order as (
        select
            coalesce(join_reason_header.salesreason_pk, 0) as salesreason_pk
            , join_reason_header.order_fk
            , coalesce(name_reason, 'no sales reason') as name_reason
            , coalesce(reasontype, 'no reason type') as reasontype
            from join_reason_header
    )

        select *
        from coalesce_order
