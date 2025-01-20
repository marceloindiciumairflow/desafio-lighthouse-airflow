with
    salesorderdetail as (
    select 
        cast(salesorderdetailid as numeric) as orderdetail_pk
        , cast(salesorderid as numeric) as order_fk
        , cast(productid as numeric) as product_fk
        , carriertrackingnumber 
        , orderqty 
        , specialofferid 
        , unitprice 
        , unitpricediscount 
        , rowguid 
        , cast(modifieddate as date) as modifieddate
    from {{ source('desafio_lh_marcelo', 'salesorderdetail') }}
    )

select *
from salesorderdetail
