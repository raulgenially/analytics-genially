{% set min_date %}
    date('2022-01-01') -- we are collecting data since '2021-12-20', so first complete month is 01-2022.
{% endset %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date, "month") }}
),

metric_lastday_of_the_month as (
    select
        *,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_premium_free_users') }}
    where date_day = last_day(date_day, month)
),
-- we consider the value of these metrics in a month as the value of the metrics on the last day of the month.

final as (
    select
        date(reference_table.date_month) as date_month,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        reference_table.broad_sector,
        reference_table.broad_role,
        coalesce(m.n_free_users, 0) as n_free_users,
        coalesce(m.n_premium_users, 0) as n_premium_users

    from reference_table
    left join metric_lastday_of_the_month as m
        on date(reference_table.date_month) = m.date_month
            and reference_table.plan = m.plan
            and reference_table.subscription = m.subscription
            and reference_table.country = m.country
            and reference_table.country_name = m.country_name
            and reference_table.broad_sector = m.broad_sector
            and reference_table.broad_role = m.broad_role
)

select * from final
