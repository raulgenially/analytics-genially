with team_members as (
    select * from {{ ref('src_genially_team_members') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
),

final as (
    select
        team_members.*,

    from team_members
    inner join users
        on team_members.user_id = users.user_id
    inner join teams
        on team_members.team_id = teams.team_id    
)

select * from final
