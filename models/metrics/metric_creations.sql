with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select
        DATE(created_at) as created_at,
        user_plan,
        user_sector, 
        user_role,
        user_market,  
        count(genially_id) as n_total_creations,
        countif(is_deleted = False) as n_active_creations

    from geniallys
    --where is_current_user = True
    group by 1, 2, 3, 4, 5
    --having created_at >= DATE(2019, 1, 1)
    order by created_at asc
)

select * from final 