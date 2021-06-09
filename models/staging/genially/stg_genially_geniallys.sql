with geniallys as (
    select
        _id as genially_id,
        typegenially as genially_type,
        typesubscription as subscription_type,        
        public as is_public,
        published as is_published,
        showinsocialprofile as is_in_social_profile,
        reusable as is_reusable,
        inspiration as is_inspiration,
        deleted as is_deleted,
        noindex as is_not_indexed,
        name,
        tags, 
        description,
        friendlyurl,
        -- Foreign keys
        idUser as user_id,
        idanalytics as analytics_id,
        reusedfrom as reused_from_id,        
        idfromtemplate as from_template_id,
        idteam as team_id,
        idteamtemplate as team_template_id,
        idfromteamtemplate as from_team_template_id,
        -- Date fields are listed at the end
        lastmodified as modified_at,
        creationtime as created_at,
        datepublished as published_at,
        lastview as last_view_at,
        datedeleted as deleted_at
   
    from {{ source('genially', 'geniallys') }}
)

select * from geniallys