with
    dim_product as (
        select 
            product_pk
            , subcategory_fk
            , name_product
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
            , sellstartdate
            , sellenddate
            , modifieddate
        from {{ ref('stg_product') }}
    )

select *
from dim_product
