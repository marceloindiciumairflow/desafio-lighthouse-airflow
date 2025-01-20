with 
    product as (
        select 
            cast(productid as numeric) as product_pk
            , cast(name as string) as name_product
            , cast(productsubcategoryid as numeric) as subcategory_fk
            , cast(productmodelid as numeric) as model_fk
            , productnumber
            , makeflag
            , finishedgoodsflag
            , color
            , safetystocklevel
            , reorderpoint 
            , standardcost
            , listprice
            , size
            , sizeunitmeasurecode
            , weightunitmeasurecode
            , weight
            , daystomanufacture
            , productline
            , class
            , style
            , cast(sellstartdate as date) as sellstartdate
            , cast(sellenddate as date) as sellenddate
            , rowguid
            , cast(modifieddate as date) as modifieddate
        from {{ source('desafio_lh_marcelo', 'product') }}
    )

select *
from product
