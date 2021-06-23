with geniallys as (
    select * from {{ ref('geniallys') }}
    where is_current_user = True
),

final as (
    select
        -- Dimensions
        DATE(created_at) as created_at,
        user_plan as plan,
        user_sector as sector, 
        user_role as role,
        user_market as market,

        -- Metrics
        count(genially_id) as total_creations,
        countif(is_deleted = False) as active_creations,
        countif(is_deleted = False and is_published = True) as published_creations,
        countif(is_deleted = False and is_published = True and is_reusable = True) as reusable_creations,

        /*
        count(distinct user_id) as users,
        count(genially_id) / count(distinct user_id) as creations_per_user,
        countif(is_deleted = False) / count(distinct user_id) as active_creations_per_user,
        countif(is_deleted = False and is_published = True) / count(distinct user_id) as published_creations_per_user,
        countif(is_deleted = False and is_published = True and is_reusable = True) / count(distinct user_id) as reusable_creations_per_user
        */

    from geniallys
    where date(created_at) >= date(2019, 1, 1)
    group by 1, 2, 3, 4, 5
    order by created_at asc
)

select * from final