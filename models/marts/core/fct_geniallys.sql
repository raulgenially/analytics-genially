with geniallys as (
    select * from {{ ref('stg_genially_geniallys') }}
),

templates as (
    select * from {{ ref('stg_genially_templates') }}
),

geniallys_templates as (
        select *
        from geniallys
        left join templates on geniallys.from_template_id = templates.template_id
)

select * from geniallys_templates
where template_type is null

/*
users as (
    select * from {{ ref('dim_users') }}
),

final as (
    select
        geniallys.genially_id,

        geniallys.genially_type,
        {{ map_subscription_code('geniallys.subscription_plan') }} as plan,
        users.plan,
        geniallys.is_published,
        geniallys.is_deleted,

        geniallys.user_id,
        users.user_id,
        geniallys.from_template_id,

        geniallys.modified_at,
        geniallys.created_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at

        from geniallys
        inner join users 
            on geniallys.user_id = users.user_id
        where DATE(geniallys.created_at) >= DATE(2019, 1, 1)  

)

select * from final

/*select *,
from geniallys
where user_id is null*/