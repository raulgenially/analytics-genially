with team_members as (
    select * from {{ source('genially', 'teammember') }}
),

role_codes as (
    select * from {{ ref('seed_team_member_role_codes') }}
),

final as (
    select
        team_members._id as team_member_id,

        team_members.email,
        team_members.role as member_role,
        role_codes.name as member_role_name,

        -- See https://github.com/Genially/scrum-genially/issues/7607
        -- Guest members have confirmedat populated when the invite
        -- is created. Check that the user_id is populated to check
        -- if the user is a Genially user (and therefore part of the team)
        (
            team_members.confirmedat is not null
            and team_members.deletedat is null
            and team_members.iduser is not null
        ) as is_part_of_the_team,

        team_members.iduser as user_id,
        team_members.idteam as team_id,

        team_members.confirmedat as confirmed_at,
        team_members.deletedat as deleted_at,
        team_members.createdat as created_at -- See https://github.com/Genially/scrum-genially/issues/7607

    from team_members
    left join role_codes
        on team_members.role = role_codes.code
    where __hevo__marked_deleted = false
)

select * from final
