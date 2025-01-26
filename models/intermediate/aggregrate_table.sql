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

    , address as (
        select 
            address_pk
            , stateprovince_fk
            , city
        from {{ ref('stg_address') }}
    )

    , state as (
        select 
            name_state
            , countryregion_fk
            , territory_fk
            , stateprovince_pk
        from {{ ref('stg_stateprovince') }}
    )

    , country as (
        select 
            country_name
            , countryregion_pk
        from {{ ref('stg_countryregion') }}
    )

    , salesorder as (
        select 
            order_pk
            , shipaddress_fk
        from {{ ref('stg_salesorderheader') }}
    )

    , territory as (
        select
            territory_name
            , territory_pk
        from {{ ref('stg_salesterritory') }}
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

    , employee as (
        select 
            cast(employee_pk as string) as employee_pk
            , nationalidnumber
            , sickleavehours
            , gender
            , hiredate
            , birthdate
        from {{ ref('stg_employee') }}
    )

    , person as (
        select
            cast(business_fk as string) as business_fk
            , fullname
            , persontype
        from {{ ref('stg_person') }}
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

     , orders_address as (
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
            , stateprovince_fk
            , city
        from orderdetail_product
        left join address
            on orderdetail_product.shipaddress_fk = address.address_pk
     )
    
        , orders_state as (
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
                , stateprovince_fk
                , city
                , name_state
                , countryregion_fk
                , territory_fk
            from orders_address
            left join state
                on orders_address.stateprovince_fk = state.stateprovince_pk
        )

        , orders_country as (
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
                , stateprovince_fk
                , city
                , name_state
                , countryregion_fk
                , territory_fk
                , country_name
            from orders_state
            left join country
                on orders_state.countryregion_fk = country.countryregion_pk
        )

    , orders_employee as (
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
                , stateprovince_fk
                , city
                , name_state
                , countryregion_fk
                , territory_fk
                , country_name
                , nationalidnumber
                , sickleavehours
                , gender
                , hiredate
                , birthdate
                , employee_pk
        from orders_country
        left join employee
            on orders_country.businessentity_fk = employee.employee_pk
    )

    , aggregrate_table as (
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
            , stateprovince_fk
            , city
            , name_state
            , countryregion_fk
            , territory_fk
            , country_name
            , nationalidnumber
            , sickleavehours
            , gender
            , hiredate
            , birthdate
            , business_fk
            , fullname
            , persontype
            , coalesce(employee_pk, 'no employee') as employee_pk
        from orders_employee
        left join person
            on orders_employee.employee_pk = person.business_fk
    )
    select *
    from aggregrate_table





