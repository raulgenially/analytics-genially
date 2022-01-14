with collaboratives as (
    -- TODO: rely on the src model once
    -- https://github.com/Genially/scrum-genially/issues/8259
    -- is addressed
    select * from {{ ref('int_stg_cleaned_collaboratives') }}
),

team_members as (
    select * from {{ ref('src_genially_team_members') }}
),

final as (
    select
        collaboratives.collaborative_id,

        collaboratives.collaboration_type,

        collaboratives.genially_id,
        if(
            collaboration_type = 1,
            collaboratives.user_id,
            team_members.user_id
        ) as user_id,
        collaboratives.user_owner_id,
        collaboratives.team_id,

        collaboratives.created_at

    from collaboratives
    -- Collaboratives user id is linked to team members when collaboration type is 4
    -- (see https://github.com/Genially/scrum-genially/issues/7287)
    left join team_members
        on collaboratives.user_id = team_members.team_member_id
)

select * from final
