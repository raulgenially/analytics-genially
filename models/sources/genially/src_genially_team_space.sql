with teamspace as (
    select * from {{ source('genially', 'teamspace') }}
),

final as (
    select
        _id as team_space_id,

        name,
        icon,

        common as is_common,

        idteam as team_id,
        owner as owner_id,

        createdat as created_at,

    from teamspace
    where __hevo__marked_deleted = false
)

select * from final
