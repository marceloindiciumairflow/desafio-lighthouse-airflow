with 
    product as (
        select 
            product_pk
            , name_product     
        from {{ ref('stg_product') }}
    )

    , orderdetail as (
        select 
            orderdetail_pk
            , order_fk
            , product_fk
            , orderqty 
            , specialofferid 
            , unitprice 
            , unitpricediscount
        from {{ ref('stg_salesorderdetail') }}
    )

    , int_product as (
        select
            product_pk
            , orderdetail_pk
            , order_fk
            , name_product
            , orderqty 
            , specialofferid 
            , unitprice 
            , unitpricediscount
            , orderqty * unitprice * (1 - orderdetail.unitpricediscount) as valor_liquido
        from orderdetail
        left join product
                on orderdetail.product_fk = product.product_pk
    )

    select *
    from int_product
