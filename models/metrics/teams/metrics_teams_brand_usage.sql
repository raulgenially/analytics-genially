with teams as (
    select * from {{ ref('teams') }}
    where is_disabled = false
),

final as (
    select
        'Has logo in team tab' as brand_usage_type,
        team_id,
        name,
        created_at,      
        countif(has_logo_in_team_tab = true) as has_usage

    from teams
    {{ dbt_utils.group_by(n=4) }}

    union all

    select
        'Has cover picture in team tab' as brand_usage_type,
        team_id,
        name,
        created_at,
        countif(has_cover_picture_in_team_tab = true) as has_usage

    from teams
    {{ dbt_utils.group_by(n=4) }}

    union all

    select
        'Has logo in team brand section' as brand_usage_type,
        team_id,
        name,
        created_at,
        countif(has_logo_in_team_brand_section = true) as has_usage

    from teams
    {{ dbt_utils.group_by(n=4) }}

    union all

    select
        'Has loader in team brand section' as brand_usage_type,
        team_id,
        name,
        created_at,
        countif(has_loader_in_team_brand_section = true) as has_usage

    from teams
    {{ dbt_utils.group_by(n=4) }}
)

select * from final
