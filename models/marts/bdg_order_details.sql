with 
    fct_sales as (
        select 
            order_pk as order_sales_fk
            , orderdetail_pk as orderdetails_sales_fk
            , product_pk as product_sales_fk
        from {{ ref('fct_sales') }}
    )

    , product as (
        select 
            product_pk 
        from {{ ref('stg_product') }}
    )

    , orders as (
        select 
            order_pk
        from {{ ref('stg_salesorderheader') }}
    )

    , aggregated_fct_sales as (
        select 
            order_sales_fk
            , min(orderdetails_sales_fk) as orderdetails_sales_fk
            , min(product_sales_fk) as product_sales_fk
        from fct_sales
        group by order_sales_fk
    )

    , bdg_order_details as (
        select distinct
            md5(concat(order_sales_fk, orderdetails_sales_fk, product_sales_fk)) as order_details_sk
            , order_sales_fk as sales_order_fk
            , product_sales_fk as sales_product_fk
        from aggregated_fct_sales
        left join orders 
            on aggregated_fct_sales.order_sales_fk = orders.order_pk
        left join product
            on aggregated_fct_sales.product_sales_fk = product.product_pk
    )

select *
from bdg_order_details
