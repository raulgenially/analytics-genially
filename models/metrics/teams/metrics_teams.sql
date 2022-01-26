with geniallys as (
    select * from {{ ref('geniallys') }}
    where space_id is not null
        and is_active = true
),

team_spaces as (
    select * from {{ ref('team_spaces') }}
),

teams as (
    select
        team_id,
        name,
        created_at

    from {{ ref('teams') }}
    where is_disabled = false
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
        date(date_spine.date_day) as date_day,
        teams.team_id,
        teams.name as team_name,
        teams.created_at as team_created_at

    from date_spine
    cross join teams
),

geniallys_by_creation_date as (
    select
        -- Dimensions
        date(created_at) as created_at,
        team_id,

        -- Metrics
        count(genially_id) as n_active_creations,

    from geniallys
    group by 1, 2
),

spaces_by_creation_date as (
    select
        -- Dimensions
        date(created_at) as created_at,
        team_id,

        -- Metrics
        count(team_space_id) as n_spaces,

    from team_spaces
    group by 1, 2
),

metrics_joined as (
    select
        -- Dimensions
        spine.date_day,
        spine.team_id,
        spine.team_name,
        spine.team_created_at,

        -- Metrics
        coalesce(geniallys_by_creation_date.n_active_creations, 0) as n_active_creations,
        coalesce(spaces_by_creation_date.n_spaces, 0) as n_spaces

    from spine
    left join geniallys_by_creation_date
        on spine.date_day = geniallys_by_creation_date.created_at
            and spine.team_id = geniallys_by_creation_date.team_id
    left join spaces_by_creation_date
        on spine.date_day = spaces_by_creation_date.created_at
            and spine.team_id = spaces_by_creation_date.team_id
),

metrics_joined_cumsum as (
    select
        *,
        sum(n_active_creations) over (partition by team_id order by date_day asc) as n_cumulative_active_creations,
        sum(n_spaces) over (partition by team_id order by date_day asc) as n_cumulative_spaces

    from metrics_joined
),

final as (
    select
        *

    from metrics_joined_cumsum
    where date_day >= '{{ var('team_feat_start_date') }}'
    order by date_day asc
)

select * from final
