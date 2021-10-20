with team_templates as (
    select * from {{ source('genially', 'teamtemplate') }}
),

final as (
    select
        _id as team_template_id,

        idgenially as genially_id,
        idteam as team_id,
        idspace as space_id,

    from team_templates
    where __hevo__marked_deleted = false
)

select * from final
