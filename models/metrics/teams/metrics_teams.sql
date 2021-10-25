with geniallys as (
    select * from {{ ref('team_geniallys') }}
),

team_spaces as (
    select * from {{ ref('team_spaces') }}
),

teams as (
    select 
        name,
        created_at
 
    from {{ ref('teams') }}
),

{% set min_date %}
    date('2018-01-01')
{% endset %}

-- Geniallys moved to a Team workspace maintain the original creation date
-- 2018 is used as start date to make sure we are including all geniallys in the subsequent calculations (cumulative sums)
-- Note we are assuming that older Geniallys are not going to be moved to a Team workspace
-- However, we'll filter out by creation date later for computational performance (see final CTE)
date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day", 
        start_date=min_date, 
        end_date="current_date()"
       )
    }}
),

spine as (
    select
        date(date_spine.date_day) as created_at,
        name as team_name,
        created_at as team_created_at

    from date_spine
    cross join teams
),

geniallys_by_creation_date as (
    select 
        -- Dimensions
        date(created_at) as created_at,
        team_name,

        -- Metrics
        countif(is_active = true) as n_active_creations,

    from geniallys
    group by 1, 2
),

spaces_by_creation_date as (
    select 
        -- Dimensions
        date(created_at) as created_at,
        team_name,

        -- Metrics
        count(team_space_id) as n_spaces,

    from team_spaces
    group by 1, 2
),

metrics_joined as (
    select
        -- Dimensions
        spine.created_at,
        spine.team_name,
        spine.team_created_at,

        -- Metrics
        coalesce(geniallys_by_creation_date.n_active_creations, 0) as n_active_creations,
        coalesce(spaces_by_creation_date.n_spaces, 0) as n_spaces

    from spine
    left join geniallys_by_creation_date
        on spine.created_at = geniallys_by_creation_date.created_at
            and spine.team_name = geniallys_by_creation_date.team_name
    left join spaces_by_creation_date
        on spine.created_at = spaces_by_creation_date.created_at
            and spine.team_name = spaces_by_creation_date.team_name
),

metrics_joined_cumsum as (
    select
        *,
        sum(n_active_creations) over (partition by team_name order by created_at) as n_cumulative_active_creations,
        sum(n_spaces) over (partition by team_name order by created_at) as n_cumulative_spaces

    from metrics_joined
),

final as (
    select 
        *
    
    from metrics_joined_cumsum
    where created_at >= '{{ var('team_feat_start_date') }}'
    order by created_at asc
)

select * from final
