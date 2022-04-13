{% set start_date_of_analysis %}
    '{{ var('start_date_of_analysis_OKR') }}'
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date=start_date_of_analysis,
        end_date="current_date()"
        )
    }}
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
        date(dates.date_month) as date_month,
        m.plan,
        m.country,
        m.country_name,
        m.broad_sector,
        m.broad_role,
        m.n_free_users,
        m.n_premium_users

    from dates
    left join metric_lastday_of_the_month as m
        on date(dates.date_month) = m.date_month
)

select * from final
