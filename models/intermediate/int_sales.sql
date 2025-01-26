with
    orderheader as (
    select 
        order_pk
        , customer_fk
        , businessentity_fk
        , territory_fk
        , billaddress_fk
        , shipaddress_fk
        , shipmethod_fk
        , creditcard_fk
        , currencyrated_fk
        , revisionnumber
        , orderdate
        , duedate
        , shipdate
        , status
        , onlineorderflag
        , purchaseordernumber
        , accountnumber
        , creditcardapprovalcode
        , subtotal
        , taxamt
        , freight
        , totaldue
        , rowguid
        , modifieddate
    from {{ ref('int_order_header') }}
    )

    , orderdetail as (
        select 
            orderdetail_pk
            , order_fk
            , product_fk
            , carriertrackingnumber
            , orderqty
            , specialofferid
            , unitprice
            , unitpricediscount
        from {{ ref('stg_salesorderdetail') }}
    )

    , join_order as (
        select
          orderheader.order_pk
        , orderheader.customer_fk
        , orderheader.businessentity_fk
        , orderheader.territory_fk
        , orderheader.billaddress_fk
        , orderheader.shipaddress_fk
        , orderheader.shipmethod_fk
        , orderheader.creditcard_fk
        , orderheader.currencyrated_fk
        , orderdetail.orderdetail_pk
        , orderdetail.order_fk
        , orderdetail.product_fk
        , orderheader.revisionnumber
        , orderheader.orderdate
        , orderheader.duedate
        , orderheader.shipdate
        , orderheader.status
        , orderheader.onlineorderflag
        , orderheader.purchaseordernumber
        , orderheader.accountnumber
        , orderheader.creditcardapprovalcode
        , orderheader.subtotal
        , orderheader.taxamt
        , orderheader.freight
        , orderheader.totaldue
        , orderheader.rowguid
        , orderheader.modifieddate
        , orderdetail.carriertrackingnumber
        , orderdetail.orderqty
        , orderdetail.specialofferid
        , orderdetail.unitprice
        , orderdetail.unitpricediscount
        , orderdetail.orderqty * orderdetail.unitprice as faturamento_bruto
        , orderdetail.orderqty * orderdetail.unitpricediscount as desconto_produto
        from orderheader
        left join orderdetail
                on orderheader.order_pk = orderdetail.order_fk
        )

    , calc_ticket_medio as (
        select
            count(distinct order_pk) as numero_pedidos
            , sum(faturamento_bruto) as faturamento_bruto
            , sum(faturamento_bruto) - sum(desconto_produto) as faturamento_liquido
        from join_order
        )

       select
        faturamento_liquido / numero_pedidos as ticket_medio
        from calc_ticket_medio
