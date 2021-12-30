with activity as (
    select * from {{ ref('metrics_login_activity') }}
),

final as (
    {{ create_metrics_activity_status_model('activity', 'status_7d') }}
)

select * from final
