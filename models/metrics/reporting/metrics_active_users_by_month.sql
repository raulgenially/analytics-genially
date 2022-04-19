{% set min_date %}
    date('2022-01-01') -- we are collecting data since '2021-12-20', so first complete month is 01-2022.
{% endset %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date, "month") }}
),

active_users as (
    select
        *,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_login_activity') }}
    where status in ('New', 'Returning')
        and date_day >= {{ min_date }}
),

returning_users as (
    select
        *,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_login_activity') }}
    where status = 'Returning'
        and date_day >= {{ min_date }}
),

active_users_reference_table as (
    select
        date(reference_table.date_month) as date_month,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        reference_table.broad_sector,
        reference_table.broad_role,
        count(distinct active_users.user_id) as n_active_users

    from reference_table
    left join active_users
        on date(reference_table.date_month) = active_users.date_month
            and reference_table.plan = active_users.plan
            and reference_table.subscription = active_users.subscription
            and reference_table.country = active_users.country
            and reference_table.country_name = active_users.country_name
            and reference_table.broad_sector = active_users.broad_sector
            and reference_table.broad_role = active_users.broad_role
    {{ dbt_utils.group_by(n=7) }}
),

returning_users_reference_table as (
    select
        date(reference_table.date_month) as date_month,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        reference_table.broad_sector,
        reference_table.broad_role,
        count(distinct returning_users.user_id) as n_returning

    from reference_table
    left join returning_users
        on date(reference_table.date_month) = returning_users.date_month
            and reference_table.plan = returning_users.plan
            and reference_table.subscription = returning_users.subscription
            and reference_table.country = returning_users.country
            and reference_table.country_name = returning_users.country_name
            and reference_table.broad_sector = returning_users.broad_sector
            and reference_table.broad_role = returning_users.broad_role
    {{ dbt_utils.group_by(n=7) }}
),

final as (
    select
        a.*,
        r.n_returning

    from active_users_reference_table as a
    left join returning_users_reference_table as r
        on a.date_month=r.date_month
            and a.plan = r.plan
            and a.subscription = r.subscription
            and a.country = r.country
            and a.country_name = r.country_name
            and a.broad_sector = r.broad_sector
            and a.broad_role = r.broad_role
)

select * from final
