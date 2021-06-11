with geniallys as (
    select
        _id as genially_id,

        typegenially as genially_type,
        typesubscription as subscription_plan,        
        name,
        tags, 
        description,
        friendlyurl as fiendly_url,

        published as is_published,
        noindex as is_private,
        public as is_password_free,
        showinsocialprofile as is_in_social_profile,
        reusable as is_reusable,
        inspiration as is_inspiration,
        deleted as is_deleted,

        idUser as user_id,
        idanalytics as analytics_id,
        reusedfrom as reused_from_id,        
        idfromtemplate as from_template_id,
        idteam as team_id,
        idteamtemplate as team_template_id,
        idfromteamtemplate as from_team_template_id,

        lastmodified as modified_at,
        creationtime as created_at,
        datepublished as published_at,
        lastview as last_view_at,
        datedeleted as deleted_at
   
    from {{ source('genially', 'geniallys') }}
)

select * from geniallys