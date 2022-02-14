with agent_groups as (
    select * from {{ source('freshdesk', 'groups') }}
),

final as (
    select
        id,

        name,
        description,

        created_at,
        updated_at

    from agent_groups
)

select * from final
