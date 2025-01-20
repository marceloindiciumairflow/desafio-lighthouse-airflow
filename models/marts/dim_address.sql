with
    dim_address as (
        select *
        from {{ ref('int_address') }}
    )

    select *
    from dim_address
