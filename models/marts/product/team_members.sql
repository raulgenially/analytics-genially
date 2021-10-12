with team_members as (
    select * from {{ ref('stg_team_members') }}
),

final as (
    select
        team_member_id,

        email,
        member_role_name as role,

        user_id,
        team_id,

        confirmed_at,
        deleted_at
    
    from team_members
)

select * from final
