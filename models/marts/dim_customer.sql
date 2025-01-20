with
    customer as (
        select *
        from {{ ref('int_customer') }}
    )

    select *
    from customer
