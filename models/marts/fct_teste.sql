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

    , creditcard as (
        select 
            creditcard_pk
            , cardnumber
            , cardtype
            , expiration_date
        from {{ ref('stg_creditcard') }}
    )

    , product as (
        select 
            product_pk
            , name_product
            , subcategory_fk
            , model_fk
            , productnumber
            , makeflag
            , finishedgoodsflag
            , color
            , safetystocklevel
            , reorderpoint 
            , standardcost
            , listprice
            , size
            , sizeunitmeasurecode
            , weightunitmeasurecode
            , weight
            , daystomanufacture
            , productline
            , class
            , style
            , sellstartdate
            , sellenddate
        from  {{ ref('stg_product') }}
    )
 
    , reason as (
        select
            order_fk
            , salesreason_fk
        from {{ ref('stg_salesorderheadersalesreason') }}
    )

    , sales_reason as (
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
            , salesreason_fk
        from orders
        left join reason
            on orders.order_pk = reason.order_fk
    )

    , order_orderdetail as (
        select 
            order_pk
            , order_fk
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
            , orderdetail_pk
            , product_fk
            , orderqty
            , unitprice
            , unitpricediscount
            , salesreason_fk
        from sales_reason
        left join orderdetail
            on sales_reason.order_pk = orderdetail.order_fk
    )

    , order_orderdetail_creditcard as ( 
        select 
            order_pk
            , creditcard_pk
            , customer_fk
            , shipaddress_fk
            , businessentity_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , order_status
            , orderdetail_pk
            , product_fk
            , orderqty
            , unitprice
            , unitpricediscount
            , cardnumber
            , cardtype
            , expiration_date
            , row_number() over (partition by order_pk, orderdetail_pk, product_fk order by order_pk) as row_orderdetail
            , orderqty * unitprice as valor_bruto
            , salesreason_fk
        from order_orderdetail
        left join creditcard
            on order_orderdetail.creditcard_fk = creditcard.creditcard_pk
    )

    , deduplicated_sales as (
        select *
        from order_orderdetail_creditcard
        where row_orderdetail = 1
    )

    , aggregated_sales as (
        select 
            order_pk as pk_order
            , sum(valor_bruto) as gross_value
            , sum(orderqty) as count_items
        from deduplicated_sales
        group by order_pk
    )

    , sales_creditcard as (
        select 
            order_pk
            , salesreason_fk
            , orderdetail_pk
            , businessentity_fk
            , product_fk
            , subcategory_fk
            , creditcard_pk
            , customer_fk
            , shipaddress_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , order_status
            , orderqty
            , unitprice
            , unitpricediscount
            , row_orderdetail
            , valor_bruto
            , name_product
            , productnumber
            , standardcost
            , listprice
            , productline
            , sellstartdate
            , sellenddate 
            , cardtype
            , expiration_date
        from deduplicated_sales
        left join product
            on deduplicated_sales.product_fk = product.product_pk
    )

    , fct_sales as ( 
        select 
            md5(concat(order_pk, orderdetail_pk, product_fk)) as sales_sk
            , order_pk
            , product_fk
            , coalesce(salesreason_fk, 0) as salesreason_fk
            , orderdetail_pk
            , subcategory_fk
            , businessentity_fk
            , creditcard_pk
            , customer_fk
            , shipaddress_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , coalesce(order_status, 'no status') as order_status
            , row_orderdetail
            , name_product
            , productnumber
            , standardcost
            , listprice
            , productline
            , sellstartdate
            , sellenddate 
            , coalesce(cardtype, 'no creditcard') as cardtype
            , expiration_date
            , orderqty
            , unitprice
            , unitpricediscount
            , count_items
            , valor_bruto
            , gross_value
            , orderqty * unitprice * (1 - unitpricediscount) as valor_liquido
            , case 
                 when count_items > 0 
                    then (orderqty * unitprice * (1 - unitpricediscount)) / count_items
                else 0
            end as ticket_medio
        from sales_creditcard
        left join aggregated_sales
            on sales_creditcard.order_pk = aggregated_sales.pk_order
    )

select *
from fct_sales
