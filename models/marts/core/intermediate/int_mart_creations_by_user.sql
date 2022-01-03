{{
    config(
        materialized='view'
    )
}}

with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
    where user_id is not null
),

creations_by_user as (
    -- Creations as collaborator
    select
        collaboratives.user_id,
        -- owner
        sum(0) as n_total_creations,
        sum(0) as n_active_creations,
        sum(0) as n_published_creations,
        -- collaborator
        countif(geniallys.is_published = true) as n_published_creations_as_collaborator,
        true as is_collaborator

    from collaboratives
    inner join geniallys
        on collaboratives.genially_id = geniallys.genially_id
    group by 1

    union all

    -- Creations as owner
    select
        user_id,
        -- owner
        count(genially_id) as n_total_creations,
        countif(is_active = true) as n_active_creations,
        countif(is_published = true) as n_published_creations,
        -- collaborator
        sum(0) as n_published_creations_as_collaborator,
        false as is_collaborator

    from geniallys
    group by 1
),

final as (
    select
        user_id,
        sum(n_total_creations) as n_total_creations,
        sum(n_active_creations) as n_active_creations,
        sum(n_published_creations) as n_published_creations,
        sum(n_published_creations_as_collaborator) as n_published_creations_as_collaborator,
        logical_or(is_collaborator) as is_collaborator

    from creations_by_user
    group by 1
)

select * from final
