with corporate_gaggles as (

    select * from {{ ref('jafflegaggle_facts') }}
    where corporate_email is not null

),

final as (

    select
        corporate_email,
        count(gaggle_id) as number_of_gaggles,
        sum(number_of_users_corporate) as number_of_users_corporate,
        min(first_event_corporate) as first_event_corporate,
        max(most_recent_event_corporate) as most_recent_event_corporate,

        sum(number_of_orders_corporate) as number_of_orders_corporate,
        min(first_order_corporate) as first_order_corporate,
        max(most_recent_order_corporate) as most_recent_order_corporate,

        sum(number_of_users) as number_of_users_associated,
        min(first_event) as first_event_associated,
        max(most_recent_event) as most_recent_event_associated,

        sum(number_of_orders) as number_of_orders_associated,
        min(first_order) as first_order_associated,
        max(most_recent_order) as most_recent_order_associated

    from corporate_gaggles
    group by 1

)

select * from final