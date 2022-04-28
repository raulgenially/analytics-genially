{% set testing_date %}
    date('2022-01-01')
{% endset %}

{% set last_day %}
    date_trunc(current_date(), month)
{% endset %}

with users_and_creations_by_day as (
    select * from {{ ref('metrics_users_and_creations_by_day') }}
),

active_users_by_month as (
    select * from {{ ref('metrics_users_and_creations_by_month') }}
),

creations_model1 as (
    select
        sum(n_creations) as n_creations

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
    and date_day < {{ last_day }}
),

creations_model2 as (
    select
        sum(n_creations) as n_creations

    from active_users_by_month
    where date_month >= {{ testing_date }}
),

creations_union as (
    select n_creations from creations_model1
    union all
    select n_creations from creations_model2

),

creations_count as (
    select
        count(distinct n_creations) as n_values

    from creations_union
),

totals_join as (
    select
        m1.n_creations as n_creations_users_and_creations_by_day,
        m2.n_creations as n_creations_active_users_by_month,
        c.n_values

    from creations_model1 as m1
        cross join creations_model2 as m2
        cross join creations_count as c
),

final as (
    select * from totals_join
    where n_values != 1
)

select * from final
