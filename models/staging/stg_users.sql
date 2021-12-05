with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_user') }}

),

renamed as (

    select
        id as user_id,
        name as user_name,
        email,
        
        {{ extract_email_domain('email') }} AS email_domain,
        
        gaggle_id, 
        created_at

    from source

)

select * from renamed