with teamspacecollaborator as (
    select * from {{ source('genially', 'teamspacecollaborator') }}
),

final as (
    select
        _id as team_space_collaborator_id,

        type as collaborator_type,

        idspace as space_id,
        spaceowner as space_owner_id,
        idteam as team_id,
        idcollaborator as collaborator_id,

        createdat as created_at,

    from teamspacecollaborator
    where __hevo__marked_deleted = false
)

select * from final
