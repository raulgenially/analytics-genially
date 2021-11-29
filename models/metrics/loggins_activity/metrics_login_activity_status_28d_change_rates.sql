with activity as (
    select * from {{ ref('metrics_login_activity') }}
),

denominator as (
    select
        date_day,
        previous_status_28d,
        count(user_id) as n_users

    from activity
    group by 1, 2
),

numerator as (
    select
        date_day,
        previous_status_28d,
        status_28d,
        count(user_id) as n_users

    from activity
    where (previous_status_28d = 'Active' and status_28d = 'Active')
         or (previous_status_28d = 'New' and status_28d = 'Active')
         or (previous_status_28d = 'Churned' and status_28d = 'Active')
    {{ dbt_utils.group_by(n=3) }}
),

final as (
    select
        denominator.date_day,
        numerator.status_28d,
        numerator.previous_status_28d,
        numerator.n_users as n_active_returning_users,
        denominator.n_users as n_total_users,
        numerator.n_users / denominator.n_users as change_rate

    from denominator
    left join numerator
        on denominator.date_day = numerator.date_day
            and denominator.previous_status_28d = numerator.previous_status_28d
)

select * from final
