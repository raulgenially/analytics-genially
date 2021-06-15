with geniallys as (
    select * from {{ ref('fct_geniallys') }}
),

final as (
    select
        role, 
        sector, 
        market, 
        plan, 
        count(genially_id) as creations

    from geniallys
    group by 1, 2, 3, 4
    order by creations desc
)

select * from final