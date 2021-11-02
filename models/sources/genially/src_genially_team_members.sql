with team_members as (
    select * from {{ source('genially', 'teammember') }}
),

role_codes as (
    select * from {{ ref('team_member_role_codes') }}
),

final as (
    select
        team_members._id as team_member_id,

        team_members.email,
        team_members.role as member_role,
        role_codes.name as member_role_name,

        team_members.iduser as user_id,
        team_members.idteam as team_id,

        team_members.confirmedat as confirmed_at,
        team_members.deletedat as deleted_at,
        team_members.createdat as created_at

    from team_members
    left join role_codes
        on team_members.role = role_codes.code
    where __hevo__marked_deleted = false
)

select * from final
