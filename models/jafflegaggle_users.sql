{% set personal_emails = get_personal_emails() %}

with users as (

    select * from {{ ref('stg_users') }}

),

events as (

    select * from {{ ref('stg_events') }}

),

user_events as (

    select
        user_id,

        min(timestamp) as first_event,
        max(timestamp) as most_recent_event,
        count(event_id) as number_of_events
    
    from events

    group by 1

),

order_events as (

    select
        user_id,

        min(timestamp) as first_order,
        max(timestamp) as most_recent_order,
        count(event_id) as number_of_orders
    
    from events
    where event_name = 'order_placed'
    group by 1

),

final as (

    select
        users.user_id,
        users.user_name,
        users.gaggle_id,
        users.email,
        user_events.first_event,
        user_events.most_recent_event,
        user_events.number_of_events,
        order_events.first_order,
        order_events.most_recent_order,
        order_events.number_of_orders,
        iff(users.email_domain in {{ personal_emails }}, null, users.email_domain)
            as corporate_email

    from users

    left join user_events
        on users.user_id = user_events.user_id

    left join order_events
        on users.user_id = order_events.user_id

)

select * from final