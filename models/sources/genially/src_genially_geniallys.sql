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
        ifnull(noindex, False) as is_private,
        ifnull(public, False) as is_password_free,
        ifnull(showinsocialprofile, False) as is_in_social_profile,
        ifnull(reusable, False) as is_reusable,
        ifnull(inspiration, False) as is_inspiration,

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
    where date(creationtime) >= date(2014, 1, 1)
        and __hevo__marked_deleted = False
)

select * from geniallys