with teams as (
    select * from {{ ref('src_genially_teams') }}
),

members as (
    select * from {{ ref('src_genially_team_members') }}
    where is_part_of_the_team = true
),

spaces as (
    select * from {{ ref('src_genially_team_spaces') }}
),

creations as (
    select * from {{ ref('int_mart_team_creations') }}
),

spaces_teams as (
    select
        team_id,
        count(team_space_id) as n_spaces

    from spaces
    group by 1
),

creations_teams as (
    select
        team_id,
        sum(n_active_creations) as n_active_creations

    from creations
    group by 1
),

members_teams as (
    select
        team_id,
        countif(member_role != 4) as n_members,
        -- We are counting only confirmed and registered guest members
        -- Guest members do not count towards the seat limit
        -- https://github.com/Genially/scrum-genially/issues/9493
        countif(member_role = 4) as n_guest_members

    from members
    group by 1
),

final as (
    select
        teams.team_id,

        teams.plan_name,
        teams.name,
        teams.seats as n_seats,
        coalesce(spaces_teams.n_spaces, 0) as n_spaces,
        coalesce(members_teams.n_members, 0) as n_members,
        coalesce(members_teams.n_guest_members, 0) as n_guest_members,
        coalesce(creations_teams.n_active_creations, 0) as n_active_creations,

        teams.is_disabled,
        teams.is_reusable_enabled,
        (teams.logo is not null) as has_logo_in_team_tab,
        (teams.banner is not null) as has_cover_picture_in_team_tab,
        (teams.branding_custom_watermark is not null) as has_logo_in_team_brand_section,
        (teams.branding_custom_logo is not null) as has_loader_in_team_brand_section,

        teams.created_at,
        teams.disabled_at

    from teams
    left join spaces_teams
        on teams.team_id = spaces_teams.team_id
    left join creations_teams
        on teams.team_id = creations_teams.team_id
    left join members_teams
        on teams.team_id = members_teams.team_id
)

select * from final
