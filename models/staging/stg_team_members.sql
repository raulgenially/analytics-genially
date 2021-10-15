with members as (
    select * from {{ ref('src_genially_team_members') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
),

final as (
    select
        members.team_member_id,

        members.email,
        members.member_role,
        members.member_role_name,
        teams.name as team_name,

        members.user_id,
        members.team_id,

        members.confirmed_at,
        members.deleted_at,
        teams.created_at as team_created_at

    from members
    inner join users
        on members.user_id = users.user_id
    inner join teams
        on members.team_id = teams.team_id    
)

select * from final
