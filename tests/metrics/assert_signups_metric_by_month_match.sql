{% set testing_date %}
    date('2022-01-01')
{% endset %}

{% set first_day_current_month %}
    date_trunc(current_date(), month)
{% endset %}

with users_and_creations_by_day as (
    select * from {{ ref('metrics_reporting_users_and_creations_by_day') }}
),

active_users_by_month as (
    select * from {{ ref('metrics_reporting_users_and_creations_by_month') }}
),

signups_model1 as (
    select
        sum(n_signups) as n_signups

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
        and date_day < {{ first_day_current_month }}
),

signups_model2 as (
    select
        sum(n_signups) as n_signups

    from active_users_by_month
    where date_month >= {{ testing_date }}
),

signups_union as (
    select n_signups from signups_model1
    union all
    select n_signups from signups_model2

),

signups_count as (
    select
        count(distinct n_signups) as n_values

    from signups_union
),

totals_join as (
    select
        m1.n_signups as n_signups_users_and_creations_by_day,
        m2.n_signups as n_signups_active_users_by_month,
        c.n_values

    from signups_model1 as m1
    cross join signups_model2 as m2
    cross join signups_count as c
),

final as (
    select * from totals_join
    where n_values != 1
)

select * from final
