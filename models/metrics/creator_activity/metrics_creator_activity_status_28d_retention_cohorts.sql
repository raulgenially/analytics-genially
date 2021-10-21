with activity as (
    select * from {{ ref('metrics_creator_activity') }}
),

monthly_activity as (
    select 
        user_id,
        first_usage_at,
        n_days_since_first_usage,
        if(status_28d = 'Churned', false, true) as is_28d_active

    from activity
    where n_days_since_first_usage in (0, 28, 56, 84, 112, 140, 168) -- 6 months
        and date_diff(current_date(), first_usage_at, day) > 168
),

final as (
    select 
        extract(year from first_usage_at) as year_of_first_usage,
        n_days_since_first_usage,
        countif(is_28d_active) / count(is_28d_active) as ratio_retained_creators
    
    from monthly_activity
    group by 1, 2
)

select * from final
