with
    salesorderheader as (
    select 
        cast(salesorderid as numeric) as order_pk 
        , cast(customerid as numeric) as customer_fk
        , cast(salespersonid as numeric) as salesperson_fk
        ,  cast(territoryid as numeric) as  territory_fk
        ,  cast(billtoaddressid as numeric) as billaddress_fk
        ,  cast(shiptoaddressid as numeric) as  shipaddress_fk
        ,  cast(shipmethodid as numeric) as  shipmethod_fk
        ,  cast(creditcardid as numeric) as creditcard_fk
        ,  cast(currencyrateid as numeric) as  currencyrated_fk
        , revisionnumber 
        , cast(orderdate as date) as orderdate
        , cast(duedate as date) as duedate
        , cast(shipdate as date) as shipdate
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
        , cast(modifieddate as date) as modifieddate
    from {{ source('desafio_lh_marcelo', 'salesorderheader') }}
    )

    , int_order_header as (
        select
            *
            , extract(year from orderdate) as order_year
            , extract(month from orderdate) as order_month
            
          
            , case
                when status = 1 then 'in process'
                when status = 2 then 'approved'
                when status = 3 then 'backordered'
                when status = 4 then 'rejected'
                when status = 5 then 'shipped'
                when status = 6 then 'canceled'
            end as order_status
    from salesorderheader
    )

    select *
    from int_order_header
