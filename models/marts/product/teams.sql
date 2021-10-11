with teams as (
    select * from {{ ref('src_genially_teams') }}
),

spaces as (
    select * from {{ ref('team_spaces') }}
),

members as (
    select * from {{ ref('src_genially_team_members') }}
),

geniallys as (
    select * from {{ ref('team_geniallys') }}
),

spaces_teams as (
    select
        team_id,
        count(team_space_id) as n_spaces,
        countif(n_active_creations > 0) as n_spaces_with_active_creations,
        count(distinct owner_id) as n_space_creators

    from spaces
    group by 1
),

members_teams as (
    select
        team_id,
        countif(confirmed_at is not null) as n_members

    from members
    group by 1
),

geniallys_teams as (
    select
        team_id,
        countif({{ define_active_creation('geniallys') }}) as n_active_creations

    from geniallys
    group by 1
), 

final as (
    select 
        teams.team_id, 

        teams.team_type_name as plan,
        teams.name,
        teams.seats as n_seats,
        coalesce(spaces_teams.n_spaces, 0) as n_spaces,
        coalesce(spaces_teams.n_spaces_with_active_creations, 0) as n_spaces_with_active_creations,
        coalesce(members_teams.n_members, 0) as n_members,
        coalesce(spaces_teams.n_space_creators, 0) as n_space_creators,
        coalesce(geniallys_teams.n_active_creations, 0) as n_active_creations,

        if(teams.logo is null, false, true) as has_logo_in_team_tab,
        if(teams.banner is null, false, true) as has_cover_picture_in_team_tab,
        if(teams.branding_custom_watermark is null, false, true) as has_logo_in_team_branding_section,
        if(teams.branding_custom_logo is null, false, true) as has_loader_in_team_branding_section,

        teams.created_at

    from teams
    left join spaces_teams
        on teams.team_id = spaces_teams.team_id
    left join members_teams
        on teams.team_id = members_teams.team_id
    left join geniallys_teams
        on teams.team_id = geniallys_teams.team_id
)

select * from final
