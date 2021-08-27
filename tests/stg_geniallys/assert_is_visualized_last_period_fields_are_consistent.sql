-- Ensure that is_visualized_last_X_days fields are consistent
-- See macro "map_genially_category"

with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

final as (
    select
        genially_id,
        is_visualized_last_90_days,
        is_visualized_last_60_days,
        is_visualized_last_30_days,
        last_view_at

    from geniallys
    where is_visualized_last_30_days = true
            and (is_visualized_last_60_days = false
                or is_visualized_last_90_days = false)
        or is_visualized_last_60_days = true
            and is_visualized_last_90_days = false
)

select * from final
