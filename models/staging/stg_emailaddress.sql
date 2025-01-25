with
    email as (
        select
            emailaddressid
            , emailaddress.emailaddress as emailaddress
            , businessentityid
            , modifieddate
            , rowguid
            , _sdc_table_version
            , _sdc_received_at
            , _sdc_sequence
            , _sdc_batched_at
        from {{ source('desafio_lh_marcelo', 'emailaddress') }}
    )

    select *
    from email
