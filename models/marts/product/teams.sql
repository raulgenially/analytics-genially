with teams as (
    select * from {{ ref('src_genially_teams') }}
),

members as (
    select * from {{ ref('team_members') }}
),

spaces as (
    select * from {{ ref('team_spaces') }}
),

spaces_teams as (
    select
        team_id,
        count(team_space_id) as n_spaces,
        sum(n_active_creations) as n_active_creations,
        countif(n_active_creations > 0) as n_spaces_with_active_creations,

    from spaces
    group by 1
),

members_teams as (
    select
        team_id,
        countif(confirmed_at is not null) as n_members,
        countif(is_owner_of_some_space = true and confirmed_at is not null) as n_space_owners

    from members
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
        coalesce(members_teams.n_space_owners, 0) as n_space_owners,
        coalesce(spaces_teams.n_active_creations, 0) as n_active_creations,

        if(teams.logo is null, false, true) as has_logo_in_team_tab,
        if(teams.banner is null, false, true) as has_cover_picture_in_team_tab,
        if(teams.branding_custom_watermark is null, false, true) as has_logo_in_team_brand_section,
        if(teams.branding_custom_logo is null, false, true) as has_loader_in_team_brand_section,

        teams.created_at

    from teams
    left join spaces_teams
        on teams.team_id = spaces_teams.team_id
    left join members_teams
        on teams.team_id = members_teams.team_id
)

select * from final
