with
    orders as (
        select 
            order_pk
            , orderdate
            , order_month
            , order_year
            , customer_fk
            , shipaddress_fk
            , subtotal
            , totaldue
            , order_status
            , creditcard_fk
        from {{ ref('int_order_header') }}
    ),

    orderdetail as (
        select 
            orderdetail_pk
            , order_fk
            , product_fk
            , orderqty
            , unitprice
            , unitpricediscount
        from {{ ref('stg_salesorderdetail') }}
    ),

    creditcard as (
        select 
            creditcard_pk
            , cardnumber
            , cardtype
            , expiration_date
        from {{ ref('stg_creditcard') }}
    ),

    row_orders as (
        select 
            orders.order_pk
            , orderdetail.orderdetail_pk
            , orderdetail.orderqty
            , row_number() over (partition by orders.order_pk order by orderdetail.order_fk) as row_orderdetail
            , orderdetail.orderqty * orderdetail.unitprice as gross_value
        from orders
        inner join orderdetail
            on orders.order_pk = orderdetail.order_fk
    ),
  
    rowsales_orders as (
        select 
            order_pk
            , count(row_orderdetail) as count_products
            , sum(gross_value) as gross_value
            , sum(orderqty) as count_items
        from row_orders
        group by order_pk
    ),

    fct_sales as (
        select      
            orders.order_pk
            , orders.orderdate
            , orders.order_month
            , orders.order_year
            , orders.customer_fk
            , orders.shipaddress_fk
            , orders.subtotal
            , orders.totaldue            
            , coalesce(orders.order_status, 'No Status') as order_status
            , rowsales_orders.count_products
            , rowsales_orders.count_items
            , rowsales_orders.gross_value      
            , coalesce(creditcard.cardtype, 'No Creditcard') as cardtype
        from orders
        left join rowsales_orders 
            on orders.order_pk = rowsales_orders.order_pk
        left join creditcard 
            on orders.creditcard_fk = creditcard.creditcard_pk
        order by orders.order_pk
    )

    select *
    from fct_sales
