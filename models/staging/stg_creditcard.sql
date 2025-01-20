with
    creditcard as (
    select 
       cast(creditcardid as numeric) as creditcard_pk  
       , cardnumber
       , cardtype  
       , date(cast(expyear as string) || '-' || lpad(cast(expmonth as string), 2, '0') || '-01') as expiration_date
    from {{ source('desafio_lh_marcelo', 'creditcard') }}
    )

select *
from creditcard
