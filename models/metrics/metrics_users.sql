with users as (
    select * from {{ ref('users') }}
),

final as (
    select 
        -- Dimensions
        date(registered_at) as registered_at,
        plan,
        sector,
        role,
        market,
        case
            when n_active_creations >= 5
                then '5+ creations'
            else concat(n_active_creations, ' creations')
        end as creations,

        -- Metrics
        count(user_id) as n_users,
        sum(n_total_creations) as n_total_creations,
        sum(n_active_creations) as n_active_creations,
        avg(days_btw_registration_and_first_creation) as days_btw_registration_and_first_creation,
        avg(days_btw_first_and_second_creation) as days_btw_first_and_second_creation,
        avg(days_btw_second_and_third_creation) as days_btw_second_and_third_creation,
        avg(days_btw_registration_and_third_creation) as days_btw_registration_and_third_creation,

    from users
    where date(registered_at) >= date(2019, 1, 1)
    group by 1, 2, 3, 4, 5, 6
    order by registered_at asc
)

select * from final