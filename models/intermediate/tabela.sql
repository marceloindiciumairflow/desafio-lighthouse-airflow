with
    orders as (
        select 
            order_pk
            , creditcard_fk
            , customer_fk
            , shipaddress_fk
            , coalesce(businessentity_fk, 'internet') as businessentity_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , order_status
        from {{ ref('int_order_header') }}
    )

    , orderdetail as (
        select 
            orderdetail_pk
            , order_fk
            , product_fk
            , orderqty
            , unitprice
            , unitpricediscount
        from {{ ref('stg_salesorderdetail') }}
    )

    , product as (
        select 
            product_pk
            , name_product
            , subcategory_fk
            , model_fk
            , productnumber
            , safetystocklevel
            , reorderpoint 
            , standardcost
            , sellenddate
        from {{ ref('stg_product') }}
    )

    , order_orderdetail as (
        select 
            orderdetail_pk
            , order_pk
            , creditcard_fk
            , customer_fk
            , shipaddress_fk
            , businessentity_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , order_status
            , product_fk
            , orderqty
            , unitprice
            , unitpricediscount
        from orderdetail
        left join orders
            on orderdetail.order_fk = orders.order_pk
    )

    , orderdetail_product as (
            select 
            orderdetail_pk
            , order_pk
            , creditcard_fk
            , customer_fk
            , shipaddress_fk
            , businessentity_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , order_status
            , product_fk
            , orderqty
            , unitprice
            , unitpricediscount
            , name_product
            , subcategory_fk
            , model_fk
        from order_orderdetail
        left join product
            on order_orderdetail.product_fk = product.product_pk
    )       

    select *
    from orderdetail_product