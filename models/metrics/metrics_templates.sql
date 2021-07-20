with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select
        -- Dimensions
        date(created_at) as created_at,
        user_plan as plan,
        user_sector as sector, 
        user_role as role,
        user_market as market,
        category,
        template_name,
       
        -- Metrics
        count(genially_id) as n_total_creations,
        countif(is_deleted = False) as n_active_creations,
        countif(is_deleted = False and is_published = True) as n_published_creations

    from geniallys
    where date(created_at) >= date(2019, 1, 1)
        and is_current_user = True
        and user_plan != 'Free' 
        and origin = 'Template'
    group by 1, 2, 3, 4, 5, 6, 7
    order by created_at asc
)

select * from final
