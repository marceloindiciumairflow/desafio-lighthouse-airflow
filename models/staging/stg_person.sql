with
    person as (
    select
        cast(businessentityid as numeric) as business_fk
        , persontype 
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

select *
from person
