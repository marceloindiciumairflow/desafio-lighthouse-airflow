with
    salesreason as (
        select 
            salesreason_pk
            , name_reason
            , reasontype 
            , modifieddate
        from {{ ref('stg_salesreason') }}
    )

    select *
    from salesreason
