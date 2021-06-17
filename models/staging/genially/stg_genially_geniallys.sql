with geniallys as (
    select
        _id as genially_id,

        typegenially as genially_type,
        {{ map_subscription_code('typesubscription') }} as subscription_plan,
        name,
        tags, 
        description,
        friendlyurl as friendly_url,

        ifnull(published, False) as is_published,
        ifnull(deleted, False) as is_deleted,
        noindex as is_private,
        public as is_password_free,
        showinsocialprofile as is_in_social_profile,
        reusable as is_reusable,
        inspiration as is_inspiration,

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
    where DATE(creationtime) >= DATE(2013, 1, 1) -- Remove very old geniallys
)

select * from geniallys