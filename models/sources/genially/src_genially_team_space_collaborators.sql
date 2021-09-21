with team_space_collaborators as (
    select * from {{ source('genially', 'teamspacecollaborator') }}
),

collaborator_codes as (
    select * from {{ ref('team_space_collaborator_type_codes') }}
),

final as (
    select
        team_space_collaborators._id as team_space_collaborator_id,

        team_space_collaborators.type as collaborator_type,
        collaborator_codes.name as collaborator_type_name,

        team_space_collaborators.idspace as space_id,
        team_space_collaborators.spaceowner as space_owner_id,
        team_space_collaborators.idteam as team_id,
        team_space_collaborators.idcollaborator as collaborator_id,

        team_space_collaborators.createdat as created_at,

    from team_space_collaborators
    left join collaborator_codes
        on team_space_collaborators.type = collaborator_codes.code
    where __hevo__marked_deleted = false
)

select * from final
