{% set min_date %}
    date('2022-01-01') -- we are collecting data since '2021-12-20', so first complete month is 01-2022.
{% endset %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date, "month") }}
),

login_activity as (
    select
        *,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_login_activity') }}
    where status in ('New', 'Returning')
        and date_day >= {{ min_date }}
),

final as (
    select
        date(reference_table.date_month) as date_month,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        reference_table.broad_sector,
        reference_table.broad_role,
        count(distinct login_activity.user_id) as n_active_users,
        count(distinct if(login_activity.status = 'New', login_activity.user_id, null)) as n_signups

    from reference_table
    left join login_activity
        on date(reference_table.date_month) = login_activity.date_month
            and reference_table.plan = login_activity.plan
            and reference_table.subscription = login_activity.subscription
            and reference_table.country = login_activity.country
            and reference_table.country_name = login_activity.country_name
            and reference_table.broad_sector = login_activity.broad_sector
            and reference_table.broad_role = login_activity.broad_role
    {{ dbt_utils.group_by(n=7) }}
)

select * from final
