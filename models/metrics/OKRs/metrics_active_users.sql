{% set min_date_for_computations %}
    date('2021-12-20')
{% endset %}

{% set max_date_for_computations %}
    date('2022-12-31')
{% endset %}

{% set start_date_of_analysis %}
    date('2022-01-01')
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = min_date_for_computations,
        end_date = max_date_for_computations
        )
    }}
),

active_users as (
    select * from {{ ref('metrics_login_activity_active_users') }}
),

final as (
    select 
        dates.date_day as date_day,
        sum(active_users.n_returning_users_28d) as active_users
    
    from dates
    left join active_users
        on date(active_users.date_day) = dates.date_day
    where dates.date_day >= {{ start_date_of_analysis }}
    group by 1
)

select * from final
