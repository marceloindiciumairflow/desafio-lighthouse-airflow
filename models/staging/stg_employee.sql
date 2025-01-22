with
    employee as (
        select
            cast(businessentityid as string) as employee_pk
            , cast(nationalidnumber as string) as nationalidnumber 
            , cast(sickleavehours as string) as sickleavehours
            , loginid
            , currentflag
            , modifieddate
            , rowguid
            , gender
            , cast(hiredate as date) as hiredate
            , salariedflag
            , cast(birthdate as date) as birthdate
            , maritalstatus
            , organizationnode
            , vacationhours
            , jobtitle
            , _sdc_table_version
            , _sdc_received_at
            , _sdc_sequence
            , _sdc_batched_at
        from {{ source("desafio_lh_marcelo", "employee") }}
    )

    , internet_employee as (
        select 
            "0" as employee_pk
            , "0" as nationalidnumber
            , "0" as sickleavehours
            , "NA" as gender
            , cast(null as date) as hiredate
            , cast(null as date) as birthdate

        from employee
        limit 1
    )

    , union_tables as (
        select
            employee_pk
            , nationalidnumber
            , sickleavehours
            , gender
            , hiredate
            , birthdate
        from internet_employee
        union all
        select
            employee_pk
            , nationalidnumber
            , sickleavehours
            , gender
            , hiredate
            , birthdate
        from employee
    )

select *
from union_tables
order by employee_pk asc