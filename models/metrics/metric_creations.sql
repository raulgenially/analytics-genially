with geniallys as (
    select * from {{ ref('fct_geniallys') }}
),

final as (
    select
        DATE(created_at) as created_at,
        role, 
        sector, 
        market, 
        plan, 
        count(genially_id) as n_creations

    from geniallys
    group by 1, 2, 3, 4, 5
    order by created_at asc
)

select * from final