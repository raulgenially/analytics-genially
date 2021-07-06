with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select
        -- Dimensions
        DATE(created_at) as created_at,
        user_plan as plan,
        user_sector as sector, 
        user_role as role,
        user_market as market,
        origin,
        category,
        if(is_current_user, 'Yes', 'No') as current_user,

        -- Metrics
        count(genially_id) as n_total_creations,
        countif(is_deleted = False) as n_active_creations,
        countif(is_deleted = False and is_collaborative = True) as n_collaborative_creations,
        countif(is_deleted = False and is_published = True) as n_published_creations

    from geniallys
    where date(created_at) >= date(2020, 1, 1)
    group by 1, 2, 3, 4, 5, 6, 7, 8
    order by created_at asc
)

select * from final