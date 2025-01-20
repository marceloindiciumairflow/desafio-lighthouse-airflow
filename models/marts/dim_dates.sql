with 
    dim_dates as (
        select 
            date_add(date('2018-01-01'), interval day_offset day) as my_date
        from 
            unnest(generate_array(0, 5479)) as day_offset -- 5479 dias (15 anos)
    )

    , dates as ( 
        select 
            my_date
            , extract(year from my_date) as year
            , extract(month from my_date) as month
            , format_timestamp('%b', timestamp(my_date)) as month_name
            , extract(day from my_date) as day
            , format_timestamp('%a', timestamp(my_date)) as day_name
            , extract(dayofyear from my_date) as day_of_year
        from dim_dates
    )

    , datas as (
        select *
            , case
                when month = 1 then 'janeiro'
                when month = 2 then 'fevereiro'
                when month = 3 then 'março'
                when month = 4 then 'abril'
                when month = 5 then 'maio'
                when month = 6 then 'junho'
                when month = 7 then 'julho'
                when month = 8 then 'agosto'
                when month = 9 then 'setembro'
                when month = 10 then 'outubro'
                when month = 11 then 'novembro'
                when month = 12 then 'dezembro'
            end as month_name_pt
        from dates
    )

select *
from datas
