{% set start_date_of_analysis %}
    '{{ var('snapshot_users_start_date') }}'
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=start_date_of_analysis,
        end_date="current_date()"
        )
    }}
),

all_users as (
    select * from {{ ref('all_users') }}
),

user_plan as (
    select * from {{ ref('user_plan_history') }}
),

user_plan_dimensiones as (
    select
        user_plan.*,
        all_users.country,
        all_users.country_name,
        all_users.broad_sector,
        all_users.broad_role

    from user_plan
    inner join all_users
        on user_plan.user_id = all_users.user_id
),

final as (
    select
        date(dates.date_day) as date_day,
        user_plan_dimensiones.plan,
        user_plan_dimensiones.country,
        user_plan_dimensiones.country_name,
        user_plan_dimensiones.broad_sector,
        user_plan_dimensiones.broad_role,
        countif(user_plan_dimensiones.subscription = 'Free') as n_free_users,
        countif(user_plan_dimensiones.subscription = 'Premium') as n_premium_users

    from dates
    left join user_plan_dimensiones
        on date(dates.date_day) between date(user_plan_dimensiones.started_at) and
        if (
             format_datetime("%H:%M:%S", user_plan_dimensiones.finished_at) = "23:59:59",
             date(user_plan_dimensiones.finished_at),
             date(user_plan_dimensiones.finished_at) - 1
        )
    {{ dbt_utils.group_by(n=6) }}
)

select * from final
