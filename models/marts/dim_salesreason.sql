with
    dim_salesreason as (
        select *
        from {{ ref('int_salesreason') }}
    )

    select *
    from dim_salesreason
