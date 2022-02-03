-- The number of signups should match among the implicated models.
with login_activity_active_users as (
    select * from {{ ref('metrics_login_activity_active_users') }}
),

monthly_projections as (
    select * from {{ ref('metrics_monthly_projections') }}
),

users_in_funnel as (
    select * from {{ ref('metrics_users_in_funnel') }}
),

signups_model1 as (
    select
        date_day,
        plan,
        country,
        sum(n_signups) as n_signups

    from login_activity_active_users
    where date_day = '2022-01-20'
        and plan = 'Free'
        and country = 'US'
    {{ dbt_utils.group_by(n=3) }}
),

signups_model2 as (
    select
        date_day,
        plan,
        country,
        sum(n_signups) as n_signups

    from monthly_projections
    where date_day = '2022-01-20'
        and plan = 'Free'
        and country = 'US'
    {{ dbt_utils.group_by(n=3) }}
),

signups_model3 as (
    select
        registered_at,
        plan,
        country,
        sum(n_signups) as n_signups

    from users_in_funnel
    where registered_at = '2022-01-20'
        and plan = 'Free'
        and country = 'US'
    {{ dbt_utils.group_by(n=3) }}
),

signups_union as (
    select n_signups from signups_model1
    union all
    select n_signups from signups_model2
    union all
    select n_signups from signups_model3
),

signups_count as (
    select
        count(distinct n_signups) as n_values

    from signups_union
),

final as (
    select * from signups_count
    where n_values != 1
)

select * from final
