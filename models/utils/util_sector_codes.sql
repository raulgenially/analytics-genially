with sector_codes as (
    select * from {{ ref('seed_sector_codes') }}
),

final as (
    select
        sector_id,
        case
            when version = 1 then concat(sector_name, ' (old)')
            else sector_name
        end as sector_name,
        broad_sector

        from sector_codes
)

select * from final
