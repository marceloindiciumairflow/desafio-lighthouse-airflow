with
    customer as (
    select 
        customer_pk
        , person_fk
        , territoryid
        , storeid
    from {{ ref('stg_customer') }}
    )

    , person as (
        select
            cast(business_fk as string) as business_fk
            , fullname
            , emailpromotion
        from {{ ref('stg_person') }}
    )

    , email as (
        select
            emailaddressid
            , cast(emailaddress as string) as emailaddress
            , cast(businessentityid as string) as businessentityid
        from {{ ref('stg_emailaddress') }}
    )

    , customer_person as (
        select 
            coalesce(customer_pk, business_fk) as customer_pk -- substitui valores nulos de customer_pk por business_fk
            , business_fk
            , storeid
            , coalesce(person.fullname, 'not registered') as name_customer
            , territoryid
            , emailpromotion 
            from customer
            left join person
                on customer.person_fk = person.business_fk
    )

    , int_customer as (
        select 
            customer_pk
            , business_fk
            , storeid
            , name_customer
            , territoryid
            , emailpromotion
            , emailaddress
            , emailaddressid
        from customer_person
        left join email
            on customer_person.business_fk = email.businessentityid
    )  

        select *
        from int_customer
        