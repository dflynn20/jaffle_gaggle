with users as (

    select * from {{ ref('jafflegaggle_users') }}

),

gaggles as (

    select * from {{ ref('stg_gaggles') }}

),

gaggle_total_facts as (

    select
        gaggles.gaggle_id,

        min(users.first_event) as first_event,
        max(users.most_recent_event) as most_recent_event,
        sum(number_of_events) as number_of_events,
        count(users.user_id) as number_of_users,
        
        min(users.first_order) as first_order,
        max(users.most_recent_order) as most_recent_order,
        sum(users.number_of_orders) as number_of_orders
    
    from users
    left join gaggles on users.gaggle_id = gaggles.gaggle_id

    group by 1

),

gaggle_domain_facts as (

    select
        gaggles.gaggle_id,

        {# This assumes that there is only one corporate_email per gaggle, needs to be modified if not the case #}
        users.corporate_email,

        min(users.first_event) as first_event,
        max(users.most_recent_event) as most_recent_event,
        sum(number_of_events) as number_of_events,
        count(users.user_id) as number_of_users,

        min(users.first_order) as first_order,
        max(users.most_recent_order) as most_recent_order,
        sum(users.number_of_orders) as number_of_orders
    
    from users
    left join gaggles on users.gaggle_id = gaggles.gaggle_id
    where users.corporate_email is not null

    group by 1, 2

),

final as (

    select
        gaggles.gaggle_id,
        gaggles.gaggle_name,
        
        gaggle_total_facts.first_event,
        gaggle_total_facts.most_recent_event,
        gaggle_total_facts.number_of_events,
        gaggle_total_facts.number_of_users,

        gaggle_total_facts.first_order,
        gaggle_total_facts.most_recent_order,
        gaggle_total_facts.number_of_orders,

        {# Note that the following can be null if there are no corporate_emails associated #}
        gaggle_domain_facts.corporate_email,
        gaggle_domain_facts.first_event as first_event_corporate,
        gaggle_domain_facts.most_recent_event as most_recent_event_corporate,
        gaggle_domain_facts.number_of_events as number_of_events_corporate,
        gaggle_domain_facts.number_of_users as number_of_users_corporate,
        
        gaggle_domain_facts.first_order as first_order_corporate,
        gaggle_domain_facts.most_recent_order as most_recent_order_corporate,
        gaggle_domain_facts.number_of_orders as number_of_orders_corporate

    from gaggles

    left join gaggle_total_facts
        on gaggles.gaggle_id = gaggle_total_facts.gaggle_id

    left join gaggle_domain_facts
        on gaggles.gaggle_id = gaggle_domain_facts.gaggle_id

)

select * from final