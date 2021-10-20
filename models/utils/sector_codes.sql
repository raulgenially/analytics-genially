with new_sector_codes as (
    select * from {{ ref('new_sector_codes') }}
),

old_sector_codes as (
    select
        sector_id,
        concat(sector_name, ' (old)') as sector_name,
        agg_sector
    
    from {{ ref('old_sector_codes') }}
),

final as (
    select * from new_sector_codes
    union distinct
    select * from old_sector_codes
)

select * from final
