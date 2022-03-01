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

geniallys as (
    select * from {{ ref('geniallys') }}
),

monthly_active_users as (

    select 
        dates.date_day as date_day,
        sum(active_users.n_monthly_active_users) as active_users
    
    from dates
    left join active_users
        on date(active_users.date_day) = dates.date_day
    group by 1
),

monthly_creations as (

    select
        dates.date_day as date_day,
        count(geniallys.genially_id) as creations   
    
    from dates
    left join geniallys
        on dates.date_day between date(geniallys.created_at) and date(geniallys.created_at)+27
    group by 1
),

final as (
    select 
        a.date_day,
        a.active_users,
        c.creations,
        if (a.active_users != 0,
            c.creations/a.active_users,
            null) as kr
    
    from monthly_active_users  as a
    left join monthly_creations  as c
        on a.date_day = c.date_day
    where a.date_day >= {{ start_date_of_analysis }} 
)

select * from final
