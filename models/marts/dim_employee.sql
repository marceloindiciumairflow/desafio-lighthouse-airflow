with
    employee as (
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

    , dim_employee as (
        select
            employee_pk
            , nationalidnumber
            , sickleavehours
            , gender
            , hiredate
            , birthdate
            , fullname
            , persontype
        from employee
        left join person
            on employee.employee_pk = person.business_fk    
    )

    select *
    from dim_employee
