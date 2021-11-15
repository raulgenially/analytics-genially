with team_groups as (
    select * from {{ source('genially', 'teamgroup') }}
),

final as (
    select
        _id as team_group_id,

        name,
        color,

        idteam as team_id,
        {{ array_from_str('members') }} as member_ids,

        createdat as created_at

    from team_groups
    where __hevo__marked_deleted = false
)

select * from final
