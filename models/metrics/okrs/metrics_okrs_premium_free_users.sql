{% set start_date_of_analysis %}
    '{{ var('start_date_of_analysis_OKR') }}'
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=start_date_of_analysis,
        end_date="current_date()"
        )
    }}
),

premium_free_users as (
    select * from {{ ref('metrics_premium_free_users') }}
),

final as (
    select
        *,
        -- to facilite the visualization and understanding of the metric we consider premium users per 10000 free users
        safe_multiply(
            safe_divide(premium_users, free_users),
            10000
        ) as kr

    from premium_free_users
)

select * from final
