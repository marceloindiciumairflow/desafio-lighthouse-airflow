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

    select *
    from salesorderheader
