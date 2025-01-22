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
            business_fk
            , fullname
        from {{ ref('stg_person') }}
    )

    , int_customer as (
        select 
            coalesce(customer_pk, business_fk) as customer_pk -- substitui valores nulos de customer_pk por business_fk
            , business_fk
            , storeid
            , coalesce(person.fullname, 'not registered') as name_customer
            , territoryid 
            from customer
            left join person
                on customer.person_fk = person.business_fk
    )

        select *
        from int_customer
        