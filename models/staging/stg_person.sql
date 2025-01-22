with
    person as (
        select
            cast(businessentityid as string) as business_fk
            , cast(persontype as string) as persontype
            , namestyle 
            , title 
            , firstname 
            , middlename 
            , lastname 
            , concat(firstname, ' ', coalesce(middlename, ''), ' ', lastname) as fullname
            , suffix 
            , emailpromotion
            , cast (rowguid as string)  as rowguid_person
            , cast (modifieddate as date) as modifieddate
        from {{ source('desafio_lh_marcelo', 'person') }}
    )

    , internet_person as (
        select 
            "0" as business_fk
            , "IT" as persontype
            , "NA" as fullname
        from person
        limit 1
    )

    , union_tables as (
        select
            business_fk
            , fullname
            , persontype
        from internet_person
        union all
        select
            business_fk
            , fullname
            , persontype
        from person
    )

select *
from union_tables
order by business_fk asc 
