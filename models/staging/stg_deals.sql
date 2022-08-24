with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('deals') }}

),

renamed as (

    select
        close_date,
        amount,
        deal_name,
        deal_stage,
        email

    from source

)

select * from renamed