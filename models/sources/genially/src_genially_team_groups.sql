with team_groups as (
    select * from {{ source('genially', 'teamgroup') }}
),

final as (
    select
        _id as team_group_id,

        {{ array_from_str('members') }} as members,
        name,
        color,

        idteam as team_id,

        createdat as created_at

    from team_groups
    where __hevo__marked_deleted = false
)

select * from final
