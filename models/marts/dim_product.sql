with
    dim_product as (
        select 
            product_pk
            , subcategory_fk
            , name_product
            , productnumber
            , makeflag
            , finishedgoodsflag
            , safetystocklevel
            , reorderpoint 
            , standardcost
            , listprice
            , daystomanufacture
            , sellstartdate
            , sellenddate
            , modifieddate
        from {{ ref('stg_product') }}
    )

select *
from dim_product

