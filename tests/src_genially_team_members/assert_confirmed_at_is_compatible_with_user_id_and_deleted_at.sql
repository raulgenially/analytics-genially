-- If confirmed_at file contains data, deleted_at field should be null and user_id field should be not null
with team_members as (
    select * from {{ ref('src_genially_team_members') }}
),

final as (
    select
        team_member_id,
        user_id,
        team_id,
        confirmed_at,
        deleted_at

    from team_members
    where confirmed_at is not null
        and (deleted_at is not null
            or user_id is null) 
)

select * from final
