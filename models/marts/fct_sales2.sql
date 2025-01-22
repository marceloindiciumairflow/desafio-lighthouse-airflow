with
    orders as (
        select 
            order_pk
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
 
    , order_orderdetail as (
        select 
           order_pk
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
            , row_number() over (partition by orders.order_pk order by orderdetail.order_fk) as row_orderdetail
            , orderdetail.orderqty * orderdetail.unitprice as valor_bruto
        from orders
        left join orderdetail
            on orders.order_pk = orderdetail.order_fk
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
            , row_orderdetail
            , valor_bruto
            , cardnumber
            , cardtype
            , expiration_date
    from order_orderdetail
    left join creditcard
        on order_orderdetail.creditcard_fk = creditcard.creditcard_pk
)

-- Agregando os valores para calcular o total bruto e a contagem de itens
    , aggregated_sales as (
        select 
            order_pk
            , sum(valor_bruto) as gross_value
            , sum(orderqty) as count_items
        from order_orderdetail_creditcard
        group by order_pk

)
    , sales_creditcard as (
        select 
            md5(concat(order_pk, orderdetail_pk)) as sales_sk
            , order_pk as orders_pk
            , orderdetail_pk
            , product_pk
            , subcategory_fk
            , creditcard_pk
            , customer_fk
            , shipaddress_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , coalesce(order_status, 'No Status') as order_status
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
            , coalesce(cardtype, 'No Creditcard') as cardtype
            , expiration_date
        from order_orderdetail_creditcard
        left join product
            on order_orderdetail_creditcard.product_fk = product.product_pk
)

    , fct_sales as (
        select 
            sales_sk
            , orders_pk
            , orderdetail_pk
            , product_pk
            , subcategory_fk
            , creditcard_pk
            , customer_fk
            , shipaddress_fk
            , orderdate
            , order_month
            , order_year
            , subtotal
            , totaldue
            , coalesce(order_status, 'No Status') as order_status
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
            , coalesce(cardtype, 'No Creditcard') as cardtype
            , expiration_date
            , gross_value
            , count_items
        from sales_creditcard
        left join aggregated_sales
            on sales_creditcard.orders_pk = aggregated_sales.order_pk
)

select *
from fct_sales
