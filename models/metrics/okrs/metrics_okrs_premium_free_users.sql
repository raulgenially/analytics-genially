{% set start_date_of_analysis %}
    '{{ var('start_date_of_analysis_OKR') }}'
{% endset %}

with premium_free_users as (
    select *
    from {{ ref('metrics_premium_free_users') }}
    where date_day >= {{ start_date_of_analysis }}
),

premium_free_users_by_day as (
    select
        date_day,
        sum(n_free_users) as n_free_users,
        sum(n_premium_users) as n_premium_users

    from premium_free_users
    group by 1
),

final as (
    select
        *,
        -- to facilite the visualization and understanding of the metric we consider premium users per 10000 free users
        safe_multiply(
            safe_divide(n_premium_users, n_free_users),
            10000
        ) as kr

    from premium_free_users_by_day
)

select * from final
