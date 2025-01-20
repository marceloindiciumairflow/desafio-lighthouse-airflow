with
    orders as (
    select 
        order_pk
        , orderdate
    from {{ ref('int_order_header') }}
    )

    , orderdetail as (
    select 
        order_fk
        , modifieddate
        , orderqty
        , unitprice
    from {{ ref('stg_salesorderdetail') }}
    )

    , creditcard as (
        select 
            creditcard_pk
            , cardnumber
            , coalesce(cardtype, 'no_credit_card') as cardtype
            , expiration_date
        from {{ ref('stg_creditcard') }}
    )

    , calculate as (
        select 
            orderqty * (unitprice) as total_price
            , orderdetail.modifieddate
            , orderdate
        from orderdetail
        left join orders
                on orders.order_pk = orderdetail.order_fk
    )

    select 
        sum(calculate.total_price) as total_vendas_brutas_2011
    from calculate
    where extract(year from orderdate) = 2011
