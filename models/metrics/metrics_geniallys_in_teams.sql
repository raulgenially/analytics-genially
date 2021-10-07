with geniallys as (
    select * from {{ ref('team_geniallys') }}
),

final as (
    select 
        -- Dimensions
        date(created_at) as created_at,
        team_name,

        -- Metrics
        countif({{ define_active_creation('geniallys') }}) as n_active_creations,

    from geniallys
    where date(created_at) >= date(2019, 1, 1) and date(created_at) < current_date() 
    group by 1, 2
    order by created_at asc
)

select * from final
