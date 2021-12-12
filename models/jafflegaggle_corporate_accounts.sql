with corporate_gaggles as (

    select * from {{ ref('jafflegaggle_facts') }}
    where corporate_email is not null

),

corporate_power_users as (
    select 
        corporate_email,
        get(array_agg(user_id) within group (order by created_at asc), 0)::int as first_user_id,
        get(array_agg(user_id) within group (order by number_of_events desc), 0)::int as most_active_user_id,
        get(array_agg(user_id) within group (order by number_of_orders desc), 0)::int as most_orders_user_id
    from {{ ref('jafflegaggle_contacts') }}
    where corporate_email is not null
    group by 1
),

final_characteristics as (

    select
        coalesce(mcd.new_domain, corporate_gaggles.corporate_email) as corporate_email,
        count(distinct(gaggle_id)) as number_of_gaggles,
        sum(number_of_users_corporate) as number_of_users_corporate,
        sum(number_of_events_corporate) as number_of_events_corporate,
        min(first_event_corporate) as first_event_corporate,
        max(most_recent_event_corporate) as most_recent_event_corporate,

        sum(number_of_orders_corporate) as number_of_orders_corporate,
        min(first_order_corporate) as first_order_corporate,
        max(most_recent_order_corporate) as most_recent_order_corporate,

        sum(number_of_users) as number_of_users_associated,
        sum(number_of_events) as number_of_events_associated,
        min(first_event) as first_event_associated,
        max(most_recent_event) as most_recent_event_associated,

        sum(number_of_orders) as number_of_orders_associated,
        min(first_order) as first_order_associated,
        max(most_recent_order) as most_recent_order_associated
        
    from corporate_gaggles
    left join {{ ref('merged_company_domain') }} mcd on corporate_gaggles.corporate_email = mcd.old_domain
    group by 1

),

final as (
    select
        fc.*,
        corporate_power_users.first_user_id,
        corporate_power_users.most_active_user_id,
        corporate_power_users.most_orders_user_id

    from final_characteristics fc
    left join corporate_power_users on fc.corporate_email = corporate_power_users.corporate_email
)

select * from final